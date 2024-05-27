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

# Stage 2: Create a minimal image with the Go binary and install Python dependencies
FROM alpine:latest

# Install Python and pip
RUN apk add --no-cache python3 py3-pip

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/fuzzymatchfinder .

# Copy configuration and other necessary files
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/assets /app/assets
COPY --from=builder /app/python-ml /app/python-ml

# Copy the requirements.txt file for the Python dependencies
COPY --from=builder /app/python-ml/requirements.txt /app/python-ml/requirements.txt

# Install Python dependencies
RUN pip3 install -r /app/python-ml/requirements.txt

# Expose the port on which the service will run
EXPOSE 8080

# Run the binary
CMD ["./fuzzymatchfinder"]
