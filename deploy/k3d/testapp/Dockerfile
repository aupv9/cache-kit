# Build from the repo root so the go.mod `replace` can reach the library:
#   docker build -f deploy/k3d/testapp/Dockerfile -t cachekit-testapp:dev .
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY . .
WORKDIR /src/deploy/k3d/testapp
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/testapp .

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/testapp /usr/local/bin/testapp
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/testapp"]
