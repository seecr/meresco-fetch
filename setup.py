#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

from os import walk, listdir
from os.path import join, isdir
data_files = []

packages = []
for path, dirs, files in walk('meresco'):
    if '__init__.py' in files:
        packagename = path.replace('/', '.')
        if packagename == 'meresco':
            continue
        packages.append(packagename)

setup(
    name='meresco-fetch',
    packages=[
        'meresco',            #DO_NOT_DISTRIBUTE
    ] + packages,
    data_files=data_files,
    scripts=[join('bin', f) for f in listdir('bin')] if isdir('bin') else [],
    version='%VERSION%',
    author='Seecr (Seek You Too B.V.)',
    author_email='info@seecr.nl',
    description='Meresco Fetch is a small framework for harvesting',
    long_description="Meresco Fetch is a small framework that provides simple components to create harvesters for various sources.",
    license='GNU Public License',
    platforms='all',
)
