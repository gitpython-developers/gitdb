GitDB
=====

GitDB allows you to access bare git repositories for reading and writing. It aims at allowing full access to loose objects as well as packs with performance and scalability in mind. It operates exclusively on streams, allowing to handle large objects with a small memory footprint.

Installation
============

.. image:: https://img.shields.io/pypi/v/gitdb.svg
    :target: https://pypi.python.org/pypi/gitdb/
    :alt: Latest Version
.. image:: https://img.shields.io/pypi/pyversions/gitdb.svg
    :target: https://pypi.python.org/pypi/gitdb/
    :alt: Supported Python versions
.. image:: https://readthedocs.org/projects/gitdb/badge/?version=latest
    :target: https://readthedocs.org/projects/gitdb/?badge=latest
    :alt: Documentation Status

From `PyPI <https://pypi.python.org/pypi/gitdb>`_

 pip install gitdb

SPEEDUPS
========

If you want to go up to 20% faster, you can install gitdb-speedups with:

 pip install gitdb-speedups

REQUIREMENTS
============

* pytest - for running the tests

SOURCE
======
The source is available in a git repository at gitorious and github:

https://github.com/gitpython-developers/gitdb

Once the clone is complete, please be sure to initialize the submodules using

 cd gitdb
 git submodule update --init

Run the tests with

 pytest

DEVELOPMENT
===========

.. image:: https://github.com/gitpython-developers/gitdb/workflows/Python%20package/badge.svg
    :target: https://github.com/gitpython-developers/gitdb/actions

The library is considered mature, and not under active development. It's primary (known) use is in git-python.

INFRASTRUCTURE
==============

* Mailing List
    * http://groups.google.com/group/git-python

* Issue Tracker
    * https://github.com/gitpython-developers/gitdb/issues

LICENSE
=======

New BSD License
