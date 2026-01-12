package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"

	"openai-agent-service/internal/config"
	"openai-agent-service/internal/handlers"
	"openai-agent-service/internal/routes"
	"openai-agent-service/internal/services"
	"openai-agent-service/internal/store"
)

func connectDB(ctx context.Context, databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func databaseDoesNotExist(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		// 3D000: invalid_catalog_name
		return string(pqErr.Code) == "3D000"
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") && strings.Contains(msg, "database")
}

func ensureDatabaseExists(ctx context.Context, cfg config.Config) error {
	u, err := url.Parse(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if strings.TrimSpace(dbName) == "" {
		return errors.New("DATABASE_URL missing database name")
	}

	maint := *u
	maint.Path = "/" + strings.TrimSpace(cfg.MaintenanceDB)
	maintDB, err := connectDB(ctx, maint.String())
	if err != nil {
		return err
	}
	defer maintDB.Close()

	var exists int
	err = maintDB.QueryRowContext(ctx, "SELECT 1 FROM pg_database WHERE datname = $1", dbName).Scan(&exists)
	if err == sql.ErrNoRows {
		exists = 0
		err = nil
	}
	if err != nil {
		return err
	}
	if exists == 1 {
		return nil
	}

	_, err = maintDB.ExecContext(ctx, "CREATE DATABASE "+pq.QuoteIdentifier(dbName))
	return err
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := connectDB(ctx, cfg.DatabaseURL)
	if err != nil {
		if cfg.AutoCreateDB && databaseDoesNotExist(err) {
			if err2 := ensureDatabaseExists(ctx, cfg); err2 != nil {
				panic(err2)
			}
			db, err = connectDB(ctx, cfg.DatabaseURL)
		}
	}
	if err != nil {
		panic(err)
	}
	defer db.Close()

	pg := store.NewPostgresStore(db)
	if err := pg.EnsureSchema(ctx); err != nil {
		panic(err)
	}

	hc := &http.Client{Timeout: 30 * time.Second}

	gateway := &services.GatewayClient{BaseURL: cfg.ToolGatewayURL, APIKey: cfg.ToolGatewayAPIKey, HTTP: hc}
	openai := &services.OpenAIClient{APIKey: cfg.OpenAIAPIKey, Model: cfg.OpenAIModel, HTTP: hc}
	catalog := services.NewToolCatalogWithKey(cfg.ToolGatewayURL, cfg.ToolGatewayAPIKey, hc, 2*time.Minute)

	chatSvc := &services.ChatService{
		MockMode:     cfg.MockMode,
		Gateway:      gateway,
		OpenAI:       openai,
		Store:        pg,
		Catalog:      catalog,
		MaxToolCalls: 6,
		MaxToolBytes: 1_000_000,
	}

	chatHandlers := &handlers.ChatHandlers{Chat: chatSvc}
	streamHandlers := &handlers.StreamHandlers{Chat: chatSvc}
	convHandlers := &handlers.ConversationHandlers{Store: pg}

	h := routes.NewRouter(cfg, chatHandlers, streamHandlers, convHandlers)

	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	addr := ":" + cfg.Port
	log.Printf("openai-agent-service listening on %s (tool_gateway=%s)", addr, cfg.ToolGatewayURL)
	if err := http.ListenAndServe(addr, h); err != nil {
		panic(err)
	}
}
