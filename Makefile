build:
	go build -o ./bin/xlsx-merger-v1.0.0-windows.exe ./cmd/xlsx-merger

build-windows:
	GOOS=windows GOARCH=amd64 go build -o ./bin/xlsx-merger.exe ./cmd/xlsx-merger

install:
	go install ./cmd/xlsx-merger

clean:
	rm -rf ./bin