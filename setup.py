#!/usr/bin/env python
from distutils.core import setup, Extension 
    
setup(name = "gitdb",
      version = "0.5.0",
      description = "Git Object Database",
      author = "Sebastian Thiel",
      author_email = "byronimo@gmail.com",
      url = "http://gitorious.org/git-python/gitdb",
      packages = ('gitdb', 'gitdb.db', 'gitdb.test', 'gitdb.test.db', 'gitdb.test.performance'),
      package_data={'gitdb' : ['AUTHORS', 'README'], 
					  'gitdb.test' : ['fixtures/packs/*', 'fixtures/objects/7b/*']},
      package_dir = {'gitdb':''},
      ext_modules=[Extension('gitdb._fun', ['_fun.c'])],
      license = "BSD License",
      requires=('async (>=0.6.0)',),
      long_description = """GitDB is a pure-Python git object database"""
      )
