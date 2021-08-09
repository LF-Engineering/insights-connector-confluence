GO_BIN_FILES=dsconfluence.go context.go error.go utils.go
#for race CGO_ENABLED=1
#GO_ENV=CGO_ENABLED=1
GO_ENV=CGO_ENABLED=0
#GO_BUILD=go build -ldflags '-s -w' -race
GO_BUILD=go build -ldflags '-s -w'
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_IMPORTS=goimports -w
GO_ERRCHECK=errcheck -asserts -ignore '[FS]?[Pp]rint*'
BINARIES=dsconfluence

all: check ${BINARIES}

dsconfluence: dsconfluence.go
	 ${GO_ENV} ${GO_BUILD} -o dsconfluence ${GO_BIN_FILES}

fmt: ${GO_BIN_FILES}
	${GO_FMT} ${GO_BIN_FILES}

lint: ${GO_BIN_FILES}
	${GO_LINT} ${GO_BIN_FILES}

vet: ${GO_BIN_FILES}
	${GO_VET} ${GO_BIN_FILES}

imports: ${GO_BIN_FILES}
	${GO_IMPORTS} ${GO_BIN_FILES}

errcheck: ${GO_BIN_FILES}
	${GO_ERRCHECK} ${GO_BIN_FILES}

check: fmt lint imports vet errcheck

swagger: clean
	swagger -q generate model -t gen -f swagger/ds-confluence.yaml
clean:
	rm -rf ${BINARIES} ./gen
	mkdir gen

.PHONY: all
