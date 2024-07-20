build:
	go build -o app .
run1: build
	./app .env_1
run2: build
	./app .env_2
run3: build
	./app .env_3
rm:
	rm app
test:
	go test -v ./...