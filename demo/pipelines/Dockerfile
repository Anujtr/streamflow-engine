FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY ../../go.mod ../../go.sum ./
RUN go mod download

# Copy source code
COPY ../../internal ./internal
COPY . ./demo/pipelines

WORKDIR /app/demo/pipelines

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o pipelines .

# Final stage
FROM alpine:3.18

RUN apk --no-cache add ca-certificates curl
WORKDIR /root/

# Copy binary from builder stage
COPY --from=builder /app/demo/pipelines/pipelines .

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the application
CMD ["./pipelines"]