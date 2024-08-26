run: build
	./bin/goredis

build:
	go build -C src -o ../bin/goredis