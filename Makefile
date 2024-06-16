run: build execute_binary

build:
	@go build -o 1brc

execute_binary:
	time ./1brc
