## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015-2016 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.fetch.harvester import BatchProtocol, RecordProtocol

from meresco.oai.tools.iterateoaipmh import OaiListRequest

class OaiPmhDownload(object):
    def __init__(self, repositories, log, recordAllowedFilter=None):
        self._repositories = repositories
        self._log = log
        self._recordAllowedFilter = (lambda record: True) if recordAllowedFilter is None else recordAllowedFilter
        self._OaiListRequest = OaiListRequest

    def downloadBatch(self, resumptionAttributes):
        repositoriesRemaining = resumptionAttributes.get('repositoriesRemaining')
        if not repositoriesRemaining:
            repositoriesRemaining = self._repositories[:]
        currentRepository = first(repositoriesRemaining)
        if currentRepository is None:
            self._log.write('no repositories configured for OaiPmhDownload.\n')
            return
        baseurl = currentRepository.get('baseurl')
        assert baseurl, "Got repository description without 'baseurl': %s" % repr(currentRepository)
        metadataPrefix = currentRepository.get('metadataPrefix')
        assert metadataPrefix, "Got repository description without 'metadataPrefix': %s" % repr(currentRepository)

        resumptionToken = resumptionAttributes.get('resumptionToken', 0)
        self._log.write("Batch download; repository: %s, resumptionToken: %s\n" % (currentRepository, resumptionToken))
        if resumptionToken:
            oaiListRequest = self._OaiListRequest(baseurl=baseurl, resumptionToken=resumptionToken)
        else:
            oaiListRequest = self._OaiListRequest(baseurl=baseurl, metadataPrefix=metadataPrefix, set=currentRepository.get('setSpec'))

        self._log.write('requesting %s\n' % oaiListRequest.buildUrl())
        oaiBatch = oaiListRequest.retrieveBatch()
        batch = _Batch(oaiBatch=oaiBatch, repositoriesRemaining=repositoriesRemaining)
        self._log.write('nrOfResults: %s, next resumptionToken: %s\n' % (len(oaiBatch.items), batch.resumptionToken))
        if not batch.resumptionToken:
            batch.repositoriesRemaining.pop(0)
            if not batch.repositoriesRemaining:
                batch.harvestingReady = True
        batch.records = [r for r in [Record(batch, item) for item in batch.oaiBatch.items] if self._recordAllowedFilter(r)]
        return batch



class _Batch(BatchProtocol):
    def __init__(self, oaiBatch, repositoriesRemaining):
        BatchProtocol.__init__(self)
        self.repositoriesRemaining = repositoriesRemaining
        currentRepository = first(repositoriesRemaining)
        self.oaiBatch = oaiBatch
        self.resumptionToken = oaiBatch.resumptionToken
        self.baseurl = oaiBatch.request.baseurl
        self.repositoryGroupId = currentRepository.get('repositoryGroupId')
        self.repositoryId = currentRepository.get('repositoryId')
        self.setSpec = currentRepository.get('setSpec') or ''
        self.metadataPrefix = currentRepository.get('metadataPrefix') or ''

    def resumptionAttributes(self):
        return {
            'resumptionToken': self.resumptionToken,
            'repositoriesRemaining': self.repositoriesRemaining
        }


class Record(RecordProtocol):
    def __init__(self, batch, item):
        self.batch = batch
        self.item = item
        self.repositoryGroupId = batch.repositoryGroupId
        self.repositoryId = batch.repositoryId
        self.setSpec = batch.setSpec
        self.metadataPrefix = batch.metadataPrefix
        self.baseurl = batch.baseurl
        self.recordIdentifier = item.identifier
        self.identifier = "%s:%s" % (batch.repositoryGroupId, self.recordIdentifier)

    def mustAdd(self):
        return not self.item.deleted

    def mustDelete(self):
        return self.item.deleted

    def asString(self):
        return str(self.item)

def first(l, default=None):
    if l:
        for v in l:
            return v
    return default
