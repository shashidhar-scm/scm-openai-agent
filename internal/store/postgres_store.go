package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/google/uuid"

	"openai-agent-service/internal/models"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS chat_conversations (
			conversation_id TEXT PRIMARY KEY,
			owner_key TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS chat_conversations_owner_key_idx ON chat_conversations(owner_key)`,
		`CREATE TABLE IF NOT EXISTS chat_messages (
			id BIGSERIAL PRIMARY KEY,
			conversation_id TEXT NOT NULL REFERENCES chat_conversations(conversation_id) ON DELETE CASCADE,
			owner_key TEXT NOT NULL,
			role TEXT NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS chat_messages_conversation_id_idx ON chat_messages(conversation_id, id)`,
		`CREATE INDEX IF NOT EXISTS chat_messages_owner_key_idx ON chat_messages(owner_key)`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) CreateConversation(ctx context.Context, ownerKey string) (models.Conversation, error) {
	id := uuid.NewString()
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO chat_conversations (conversation_id, owner_key) VALUES ($1, $2)`,
		id, ownerKey,
	); err != nil {
		return models.Conversation{}, err
	}
	return s.GetConversation(ctx, ownerKey, id)
}

func (s *PostgresStore) GetConversation(ctx context.Context, ownerKey, conversationID string) (models.Conversation, error) {
	var c models.Conversation
	err := s.db.QueryRowContext(ctx,
		`SELECT conversation_id, created_at, updated_at FROM chat_conversations WHERE owner_key = $1 AND conversation_id = $2`,
		ownerKey, conversationID,
	).Scan(&c.ConversationID, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

func (s *PostgresStore) touchConversation(ctx context.Context, ownerKey, conversationID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE chat_conversations SET updated_at = NOW() WHERE owner_key = $1 AND conversation_id = $2`,
		ownerKey, conversationID,
	)
	if err != nil {
		return err
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *PostgresStore) AppendMessage(ctx context.Context, ownerKey, conversationID, role, content string) error {
	if strings.TrimSpace(conversationID) == "" {
		return nil
	}
	if err := s.touchConversation(ctx, ownerKey, conversationID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if _, err2 := s.db.ExecContext(ctx,
				`INSERT INTO chat_conversations (conversation_id, owner_key) VALUES ($1, $2)`,
				conversationID, ownerKey,
			); err2 != nil {
				return err2
			}
		} else {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO chat_messages (conversation_id, owner_key, role, content) VALUES ($1, $2, $3, $4)`,
		conversationID, ownerKey, role, content,
	)
	return err
}

func (s *PostgresStore) ListMessages(ctx context.Context, ownerKey, conversationID string, limit int) ([]models.Message, error) {
	if strings.TrimSpace(conversationID) == "" {
		return []models.Message{}, nil
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, conversation_id, role, content, created_at
		 FROM chat_messages
		 WHERE owner_key = $1 AND conversation_id = $2
		 ORDER BY id DESC
		 LIMIT $3`,
		ownerKey, conversationID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]models.Message, 0)
	for rows.Next() {
		var m models.Message
		if err := rows.Scan(&m.ID, &m.ConversationID, &m.Role, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, m)
	}
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	return items, nil
}
