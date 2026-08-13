# syntax=docker/dockerfile:1

FROM golang:1.26.5-bookworm AS builder

WORKDIR /src

# Install ZeroMQ build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libzmq3-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*



# Copy go.mod and go.sum, tidy dependencies
COPY go.mod go.sum ./
RUN go mod tidy

# Copy the rest of the source code (excluding vendor)
COPY . .

# Vendor dependencies inside the container
RUN go mod vendor

ARG BUILD_TIME=unknown
ARG BUILD_VERSION=v0.0.0-dev

RUN CGO_ENABLED=1 go build \
    -mod=vendor \
    -trimpath \
    -ldflags="-s -w -X main.buildTime=${BUILD_TIME} -X main.buildVersion=${BUILD_VERSION}" \
    -o /out/goPool .

FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libzmq5 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/goPool /app/goPool
COPY data /app/data
COPY documentation /app/documentation

EXPOSE 3333 80 443

ENTRYPOINT ["/app/goPool"]
