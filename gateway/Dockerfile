FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/server

FROM alpine:3.22
RUN addgroup -S gateway && adduser -S -G gateway gateway
COPY --from=build /out/gateway /usr/local/bin/gateway
USER gateway
EXPOSE 8090
ENTRYPOINT ["gateway"]
