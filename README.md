# Archive Notice

The entire history of this repository is now contained in https://github.com/gitpython-developers/GitPython, where it is now maintained.
For issues, please use its issue tracker as well.

# GitDB

GitDB allows you to access bare git repositories for reading and writing. It aims at allowing full access to loose objects as well as packs with performance and scalability in mind. It operates exclusively on streams, allowing to handle large objects with a small memory footprint.

## Installation

[![Latest Version](https://img.shields.io/pypi/v/gitdb.svg)](https://pypi.python.org/pypi/gitdb/)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/gitdb.svg)](https://pypi.python.org/pypi/gitdb/)
[![Documentation Status](https://readthedocs.org/projects/gitdb/badge/?version=latest)](https://readthedocs.org/projects/gitdb/?badge=latest)

From [PyPI](https://pypi.python.org/pypi/gitdb):

```shell
pip install gitdb
```

## Speedups

If you want to go up to 20% faster, you can install gitdb-speedups with:

```shell
pip install gitdb-speedups
```

However, please note that gitdb-speedups is not currently maintained.

## Requirements

* smmap — declared as a dependency, automatically installed
* pytest — for running the tests

## Source

The source is available in a git repository on GitHub:

https://github.com/gitpython-developers/gitdb

Once the clone is complete, please be sure to initialize the submodule using:

```shell
cd gitdb
git submodule update --init
```

Run the tests with:

```shell
pytest
```

## Development

[![Python package](https://github.com/gitpython-developers/gitdb/workflows/Python%20package/badge.svg)](https://github.com/gitpython-developers/gitdb/actions)

The library is considered mature, and not under active development. Its primary (known) use is in GitPython.

## Infrastructure

* Discussions
  * https://github.com/gitpython-developers/GitPython/discussions
* Issue Tracker
  * https://github.com/gitpython-developers/gitdb/issues

## License

New BSD License
