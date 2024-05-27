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

# Install necessary packages
RUN apk add --no-cache python3 py3-pip postgresql-dev build-base gfortran openblas-dev
RUN apk add --no-cache python3-dev py3-setuptools gcc musl-dev libffi-dev

# Set the working directory
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/fuzzymatchfinder .

# Copy the Python ML script and requirements
COPY --from=builder /app/python-ml /app/python-ml

# Create and activate a virtual environment for Python
RUN python3 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

# Install Python dependencies
RUN /app/venv/bin/pip install --no-cache-dir -r /app/python-ml/requirements.txt

# Expose the port on which the service will run
EXPOSE 8080

# Run the binary
CMD ["./fuzzymatchfinder"]
