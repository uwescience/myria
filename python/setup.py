#!/usr/bin/env python

from setuptools import setup, find_packages



setup(name='MyriaPythonWorker',
      version='0.31',
      description='Myria Python worker.',
      author='Parmita Mehta',
      packages=find_packages(),
      scripts=[],
      url='https://github.com/uwescience/myria',
      setup_requires=["requests>=2.5.1"],
      install_requires=["pip >= 1.5.6"],
      classifiers=[ 'Development Status :: 3 - Alpha'  ]
     )
