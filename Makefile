all: swagger
swagger: clean
	swagger -q generate model -t gen -f swagger/ds-confluence.yaml
clean:
	rm -rf ./bin ./gen
	mkdir gen
