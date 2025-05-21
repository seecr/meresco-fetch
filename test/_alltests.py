#!/usr/bin/env python
# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015-2016, 2025 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2016 Drents Archief http://www.drentsarchief.nl
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

from os import system  # DO_NOT_DISTRIBUTE

system('find .. -name "*.pyc" | xargs rm -f')  # DO_NOT_DISTRIBUTE
from seecrdeps import includeParentAndDeps  # DO_NOT_DISTRIBUTE

includeParentAndDeps(__file__, scanForDeps=True)  # DO_NOT_DISTRIBUTE
import seecr_initvm

seecr_initvm.initvm("meresco_lucene", "meresco_oai")
from unittest import main

from harvesttest import HarvestTest
from oaipmhdownloadtest import OaiPmhDownloadTest

if __name__ == "__main__":
    main()
