#!/usr/bin/env python
# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Fetch" is a small frame work to build simple, custom harvesters.
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
#
# This file is part of "Meresco Fetch"
#
# "Meresco Fetch" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco Fetch" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco Fetch"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

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
