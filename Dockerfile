FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o addressmatchpro ./cmd/addressmatchpro

FROM python:3.10-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc g++ postgresql-server-dev-all && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/addressmatchpro .
COPY config.yaml . 

RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --no-cache --upgrade pip setuptools wheel

COPY python-ml/requirements.txt /app/python-ml/requirements.txt
RUN /app/venv/bin/pip install --no-cache-dir -r /app/python-ml/requirements.txt

EXPOSE 8080

CMD ["./addressmatchpro"]
