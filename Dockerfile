# Stage 1: Build the Go binary
FROM golang:1.25.1-alpine AS builder

# Define a build argument for the application name
ARG APP_NAME

WORKDIR /app

# Copy module files and download dependencies
COPY go.mod go.sum ./
# Also copy the shared directory to have access to its packages
COPY shared ./shared
RUN go mod download

# Copy the rest of the application source code
# This copies the entire project context, including all app folders
COPY . .

# Build the specified application
# The APP_NAME arg is used here to select the correct main package
RUN CGO_ENABLED=0 go build -o /app/server ./$APP_NAME

# Stage 2: Create the final, minimal image
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/server .

# Set the command to run the application
CMD ["./server"]
