.PHONY: lint, start, stop, test

default: lint test

lint:
	$(CURDIR)/bin/lint.sh

test: start
	$(CURDIR)/bin/test.sh $(test)
	$(CURDIR)/bin/podman_down.sh

start:
	$(CURDIR)/bin/podman_init.sh
	$(CURDIR)/bin/podman_up.sh
