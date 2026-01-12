package services

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"net/textproto"
	"os"
	"strings"
)

func gwDebugEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("GO_LOG")))
	return v == "debug" || v == "1" || v == "true"
}

func gwDebugLogf(format string, args ...any) {
	if !gwDebugEnabled() {
		return
	}
	log.Printf(format, args...)
}

type GatewayClient struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

func (c *GatewayClient) buildURL(path string) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return "", errors.New("tool gateway base url is empty")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + path, nil
}

func (c *GatewayClient) Get(path string) (int, []byte, error) {
	u, err := c.buildURL(path)
	if err != nil {
		return 0, nil, err
	}

	gwDebugLogf("gateway %s %s", http.MethodGet, u)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Accept", "application/json")

	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		gwDebugLogf("gateway %s %s -> err=%v", http.MethodGet, u, err)
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	gwDebugLogf("gateway %s %s -> status=%d bytes=%d", http.MethodGet, u, resp.StatusCode, len(b))
	return resp.StatusCode, b, nil
}

type MultipartFile struct {
	FieldName   string `json:"field_name"`
	FileName    string `json:"file_name"`
	ContentType string `json:"content_type"`
	Base64      string `json:"base64"`
}

type MultipartPayload struct {
	Fields map[string][]string `json:"fields"`
	Files  []MultipartFile     `json:"files"`
}

func (c *GatewayClient) DoMultipart(method, path string, query map[string]string, payload MultipartPayload) (int, []byte, error) {
	u, err := c.buildURL(path)
	if err != nil {
		return 0, nil, err
	}
	if len(query) > 0 {
		parsed, err := url.Parse(u)
		if err == nil {
			q := parsed.Query()
			for k, v := range query {
				q.Set(k, v)
			}
			parsed.RawQuery = q.Encode()
			u = parsed.String()
		}
	}

	gwDebugLogf("gateway %s %s", strings.ToUpper(strings.TrimSpace(method)), u)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	for k, vs := range payload.Fields {
		for _, v := range vs {
			_ = mw.WriteField(k, v)
		}
	}

	for _, f := range payload.Files {
		field := strings.TrimSpace(f.FieldName)
		if field == "" {
			field = "files"
		}
		name := strings.TrimSpace(f.FileName)
		if name == "" {
			name = "upload"
		}
		ct := strings.TrimSpace(f.ContentType)
		if ct == "" {
			ct = "application/octet-stream"
		}
		data, err := base64.StdEncoding.DecodeString(strings.TrimSpace(f.Base64))
		if err != nil {
			return 0, nil, err
		}
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition", `form-data; name="`+field+`"; filename="`+name+`"`)
		h.Set("Content-Type", ct)
		part, err := mw.CreatePart(h)
		if err != nil {
			return 0, nil, err
		}
		if _, err := part.Write(data); err != nil {
			return 0, nil, err
		}
	}

	if err := mw.Close(); err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequest(strings.ToUpper(method), u, &buf)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", mw.FormDataContentType())

	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		gwDebugLogf("gateway %s %s -> err=%v", strings.ToUpper(strings.TrimSpace(method)), u, err)
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	gwDebugLogf("gateway %s %s -> status=%d bytes=%d", strings.ToUpper(strings.TrimSpace(method)), u, resp.StatusCode, len(b))
	return resp.StatusCode, b, nil
}

func (c *GatewayClient) DoJSON(method, path string, query map[string]string, body any) (int, []byte, error) {
	u, err := c.buildURL(path)
	if err != nil {
		return 0, nil, err
	}
	if len(query) > 0 {
		parsed, err := url.Parse(u)
		if err == nil {
			q := parsed.Query()
			for k, v := range query {
				q.Set(k, v)
			}
			parsed.RawQuery = q.Encode()
			u = parsed.String()
		}
	}

	gwDebugLogf("gateway %s %s", strings.ToUpper(strings.TrimSpace(method)), u)

	var rbody io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rbody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(strings.ToUpper(method), u, rbody)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-API-Key", c.APIKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		gwDebugLogf("gateway %s %s -> err=%v", strings.ToUpper(strings.TrimSpace(method)), u, err)
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	gwDebugLogf("gateway %s %s -> status=%d bytes=%d", strings.ToUpper(strings.TrimSpace(method)), u, resp.StatusCode, len(b))
	return resp.StatusCode, b, nil
}
