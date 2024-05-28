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
RUN go build -o addressmatchpro ./cmd/addressmatchpro

# Stage 2: Create a minimal image with the Go binary
FROM python:3.10-slim-buster

# Set the working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc g++ postgresql-server-dev-all && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /app/addressmatchpro .
COPY config.yaml . 

# Create a virtual environment and install Python dependencies
RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --no-cache --upgrade pip setuptools wheel

# Copy Python requirements and install dependencies
COPY python-ml/requirements.txt /app/python-ml/requirements.txt
RUN /app/venv/bin/pip install --no-cache-dir -r /app/python-ml/requirements.txt

# Expose the port on which the service will run
EXPOSE 8080

# Run the binary
CMD ["./addressmatchpro"]
