# Stage 1: Build the Go binary
FROM golang:alpine AS builder

WORKDIR /app
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o myapp

# Stage 2: Create minimal image
FROM scratch

COPY --from=builder /app/myapp /myapp
ENTRYPOINT ["/myapp"]