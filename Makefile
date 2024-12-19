.PHONY: covreport

covreport:
	go install github.com/cancue/covreport@latest
	mkdir -p coverage
	go test -coverprofile=coverage/c.out -v ./...
	covreport -i coverage/c.out -o coverage/cover.html
	open coverage/cover.html
