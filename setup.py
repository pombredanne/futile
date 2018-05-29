#!/usr/bin/env python3

from setuptools import setup
from sys import version_info

if version_info < (3, 5, 0):
    raise SystemExit('Sorry! futile requires python 3.5.0 or later.')

setup(
    name='futile',
    description='futile - a collection of utility functions',
    long_description='futile is a collection of utility functions',
    license='MIT',
    version='0.1.0',
    author='Yifei Kong',
    url='https://github.com/yifeikong/futile',
    packages=['futile'],
    install_requires=['requests', 'pillow', 'lxml', 'pyyaml'],
    tests_require=['pytest'],
    classifiers=['Programming Language :: Python :: 3']
)
