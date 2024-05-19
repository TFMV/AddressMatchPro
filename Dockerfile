# Stage 1: Build the Go application
FROM golang:1.22-alpine AS builder

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the Go application
RUN go build -o fuzzymatchfinder ./cmd/fuzzymatchfinder

# Stage 2: Create a minimal image with the Go binary
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/fuzzymatchfinder .

# Expose the port on which the service will run
EXPOSE 8080

# Run the binary
CMD ["./fuzzymatchfinder"]
