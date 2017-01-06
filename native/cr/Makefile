CRYSTAL_BIN ?= $(shell which crystal)
ICR_BIN ?= $(shell which icr)
PREFIX ?= /usr/local

build:
	$(CRYSTAL_BIN) build --release -o test/test-search lib/util.cr test/test-search.cr $(CRFLAGS)
clean:
	rm -f ./test/test-search
test: build
	$(CRYSTAL_BIN) spec
# install: build
# 	mkdir -p $(PREFIX)/bin
# 	cp ./bin/icr $(PREFIX)/bin
# reinstall: build
# 	cp ./bin/icr $(ICR_BIN) -rf
