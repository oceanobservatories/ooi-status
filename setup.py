#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

version = '0.0.01'

setup(name='ooi-status',
      version=version,
      description='OOINet Status',
      url='https://github.com/oceanobservatories/ooi-status',
      license='BSD',
      author='Ocean Observatories Initiative',
      author_email='help@oceanobservatories.org',
      keywords=['ooistatus'],
      packages=find_packages(),
      dependency_links=[
      ],
      )
