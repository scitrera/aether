# Build stage
# Build context must be the repo root: docker build -f server/Dockerfile .
FROM golang:1.25.10-alpine AS builder

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Cache dependency downloads separately from source
COPY server/go.mod server/go.sum ./server/
COPY api/go.mod api/go.sum ./api/
COPY sdk/go/go.mod sdk/go/go.sum ./sdk/go/
RUN cd server && go mod download

COPY server/ ./server/
COPY api/ ./api/
COPY sdk/go/ ./sdk/go/

# Build all binaries as static (no CGO) with stripped debug info
RUN cd server && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/cleanup ./cmd/cleanup && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/migrate ./cmd/migrate && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/auth-proxy ./cmd/auth-proxy && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/init-secrets ./cmd/init-secrets && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/workflow ./cmd/workflow && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/msgbridge ./cmd/msgbridge && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/aetherlite ./cmd/aetherlite && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/proxy-sidecar ./cmd/proxy-sidecar

# Runtime stage — distroless-style minimal image
FROM alpine:3.23.4

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 aether && \
    adduser -D -u 1000 -G aether aether && \
    mkdir -p /etc/aether && chown aether:aether /etc/aether

COPY --from=builder /out/gateway /usr/local/bin/gateway
COPY --from=builder /out/cleanup /usr/local/bin/cleanup
COPY --from=builder /out/migrate /usr/local/bin/migrate
COPY --from=builder /out/auth-proxy /usr/local/bin/auth-proxy
COPY --from=builder /out/init-secrets /usr/local/bin/init-secrets
COPY --from=builder /out/workflow /usr/local/bin/workflow
COPY --from=builder /out/msgbridge /usr/local/bin/msgbridge
COPY --from=builder /out/aetherlite /usr/local/bin/aetherlite
COPY --from=builder /out/proxy-sidecar /usr/local/bin/proxy-sidecar

USER aether

# gRPC (50051), Ops/Health/Metrics (9090), Admin (31880), Auth Proxy (8080)
EXPOSE 50051 9090 31880 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://localhost:9090/health/live || exit 1

ENTRYPOINT ["gateway"]
CMD ["-config", "/etc/aether/config.yaml"]
