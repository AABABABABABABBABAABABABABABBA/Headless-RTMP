FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rtmpd ./cmd/rtmpd

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/rtmpd /rtmpd
EXPOSE 1935 8080
USER nonroot:nonroot
ENTRYPOINT ["/rtmpd"]
