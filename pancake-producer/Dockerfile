# Stage 1: Build the Go binary
FROM golang:1.25.1-alpine AS builder

WORKDIR /app

# Copy module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the application for the pancake-producer
RUN CGO_ENABLED=0 go build -o /app/server ./pancake-producer

# Stage 2: Create the final, minimal image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/server .

# Set the command to run the application
CMD ["./server"]
