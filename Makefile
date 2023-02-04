.PHONY: lint, start, stop, test

default: lint test

lint:
	$(CURDIR)/bin/lint.sh

test: start
	$(CURDIR)/bin/test.sh $(test)
	$(CURDIR)/bin/podman_stop.sh

start:
	$(CURDIR)/bin/podman_init.sh
	$(CURDIR)/bin/podman_start.sh

stop:
	$(CURDIR)/bin/podman_stop.sh
