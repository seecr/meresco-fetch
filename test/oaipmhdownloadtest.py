## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2016 Drents Archief http://www.drentsarchief.nl
# Copyright (C) 2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.fetch.oaipmhdownload import OaiPmhDownload
from StringIO import StringIO

class OaiPmhDownloadTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.log = StringIO()
        self.oaiListRequest = CallTrace()
        self.oaiListRequest.returnValues['create'] = self.oaiListRequest
        self.oaiListRequest.returnValues['buildUrl'] = 'http://example.org/oai?verb=ListRequest'
        self.oaiBatch = CallTrace()
        self.oaiListRequest.returnValues['retrieveBatch'] = self.oaiBatch
        self.oaiBatch.request = self.oaiBatch
        self.oaiBatch.baseurl = 'http://base.example.org'
        self.oaiBatch.resumptionToken = 'continueHere'
        self.record = CallTrace()
        self.record.identifier = 'identifier'
        self.oaiBatch.items = [self.record]

    def prepareDownload(self, repositories, **kwargs):
        dl = OaiPmhDownload(repositories=repositories, log=self.log, **kwargs)
        dl._OaiListRequest = self.oaiListRequest.create
        return dl

    def testNoRepositories(self):
        dl = self.prepareDownload(repositories=[])
        dl.downloadBatch({})
        self.assertEquals([], self.oaiListRequest.calledMethodNames())

    def testOneRepository(self):
        repo1 = {
                'baseurl': 'http://example.org/oai',
                'metadataPrefix': 'prefix',
                'repositoryGroupId': 'group'
        }
        dl = self.prepareDownload(repositories=[repo1])
        batch = dl.downloadBatch({})
        self.assertEquals(['create', 'buildUrl', 'retrieveBatch'], self.oaiListRequest.calledMethodNames())
        self.assertEquals({'baseurl': 'http://example.org/oai', 'metadataPrefix': 'prefix', 'set': None}, self.oaiListRequest.calledMethods[0].kwargs)
        self.assertEquals({'repositoriesRemaining': [repo1], 'resumptionToken': 'continueHere'}, batch.resumptionAttributes())
        self.assertEquals(["group:identifier"], [r.identifier for r in batch.records])
        self.oaiListRequest.calledMethods.reset()
        batch = dl.downloadBatch(batch.resumptionAttributes())
        self.assertEquals(['create', 'buildUrl', 'retrieveBatch'], self.oaiListRequest.calledMethodNames())
        self.assertEquals({'baseurl': 'http://example.org/oai', 'resumptionToken':'continueHere'}, self.oaiListRequest.calledMethods[0].kwargs)


    def testOneRepositoryFinishing(self):
        self.oaiBatch.resumptionToken = None
        repo1 = {
                'baseurl': 'http://example.org/oai',
                'metadataPrefix': 'prefix',
                'repositoryGroupId': 'group'
        }
        dl = self.prepareDownload(repositories=[repo1])
        batch = dl.downloadBatch({})
        self.assertEquals({'repositoriesRemaining': [], 'resumptionToken': None}, batch.resumptionAttributes())
        self.assertEquals('prefix', batch.metadataPrefix)


