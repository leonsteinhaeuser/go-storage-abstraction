coverprofile=cover.out
test:
	go test -race -cover ./...

webtest:
	go test -race -coverprofile ${coverprofile} ./...
	go tool cover -html ${coverprofile}