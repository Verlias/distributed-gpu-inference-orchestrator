FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/registry-api ./cmd/server

FROM alpine:3.22
RUN addgroup -S registry && adduser -S -G registry registry
COPY --from=build /out/registry-api /usr/local/bin/registry-api
USER registry
EXPOSE 8081
ENTRYPOINT ["registry-api"]
