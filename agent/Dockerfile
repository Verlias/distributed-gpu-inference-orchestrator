FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker-agent ./cmd/server

FROM alpine:3.22
RUN addgroup -S agent && adduser -S -G agent agent
COPY --from=build /out/worker-agent /usr/local/bin/worker-agent
USER agent
ENTRYPOINT ["worker-agent"]
