PYTHON = python
SETUP = $(PYTHON) setup.py
TESTRUNNER = $(shell which nosetests)
TESTFLAGS =

all: build

doc:: 
	make -C doc/ html

build::
	$(SETUP) build
	$(SETUP) build_ext -i

install::
	$(SETUP) install

clean::
	$(SETUP) clean --all
	rm -f *.so

coverage:: build
	PYTHONPATH=. $(PYTHON) $(TESTRUNNER) --cover-package=dulwich --with-coverage --cover-erase --cover-inclusive gitdb

