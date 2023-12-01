FROM golang:1.20 AS BuildStage

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build

FROM alpine:latest AS production

WORKDIR /app

COPY --from=BuildStage /app .

CMD ["./worker-template"]