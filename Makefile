build:
	go build -o app . && rm app

test:
	go test -v ./...