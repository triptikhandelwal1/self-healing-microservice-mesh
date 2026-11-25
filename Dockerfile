FROM golang:1.24-alpine AS builder
WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN go build -o /controlplane ./cmd/controlplane
RUN go build -o /service ./cmd/service

FROM alpine:latest
WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /controlplane /app/controlplane
COPY --from=builder /service /app/bin/service

EXPOSE 8080

CMD ["sh", "-c", "/app/controlplane -addr 0.0.0.0:${PORT:-8080}"]
