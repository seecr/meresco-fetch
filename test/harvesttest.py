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

from seecr.test import SeecrTestCase, CallTrace

from meresco.fetch import Harvester
from StringIO import StringIO

class HarvestTest(SeecrTestCase):
    def testHarvestNoRecords(self):
        log = StringIO()
        h = Harvester(self.tempdir, log=log)
        h._now = lambda: "1976-11-08T12:34:56Z"
        observer = CallTrace()
        h.addObserver(observer)

        class Batch(object):
            def __init__(self):
                self.records = []
            def getState(self):
                return {}
            def isCompleted(self):
                return True

        observer.returnValues['downloadBatch'] = Batch()

        h.harvest()
        self.assertEqual('Harvesting records with current date: 1976-11-08T00:00:00Z\n0 added, 0 deleted, 0 skipped.\n-\n', log.getvalue())

