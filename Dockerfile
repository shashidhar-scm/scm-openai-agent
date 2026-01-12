FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/openai-agent-service ./cmd/api

FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /out/openai-agent-service /openai-agent-service

EXPOSE 8091

ENV GO_LOG=debug

USER nonroot:nonroot

ENTRYPOINT ["/openai-agent-service"]
