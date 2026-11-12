# syntax=docker/dockerfile:1.7

# Stage 1: Static Go 1.26.10 Control-Plane Builder
FROM golang:1.26.10-alpine AS builder

WORKDIR /src

# Copy dependency manifests first for layer caching
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy control-plane source, embedded migrations, and OpenAPI spec
COPY cmd/ cmd/
COPY internal/ internal/
COPY api/ api/
COPY migrations/ migrations/

# Build statically linked, symbol-stripped binary with zero CGO
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/coordinator ./cmd/coordinator

# Stage 2: Minimal Distroless Non-Root Runtime (UID/GID 65532, zero shell)
FROM gcr.io/distroless/static:nonroot

WORKDIR /

COPY --from=builder /out/coordinator /coordinator
COPY --from=builder /src/api/openapi/openapi.yaml /api/openapi/openapi.yaml

ENV TMPDIR=/tmp \
    HOME=/tmp \
    OPENAPI_SPEC_PATH=api/openapi/openapi.yaml \
    HTTP_ADDR=:8080 \
    GRPC_ADDR=:50051

USER 65532:65532

EXPOSE 8080 50051

ENTRYPOINT ["/coordinator"]

