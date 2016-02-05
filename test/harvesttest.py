## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from os.path import join
from StringIO import StringIO

from simplejson import load as jsonLoad

from seecr.test import SeecrTestCase, CallTrace
from seecr.zulutime import ZuluTime

from meresco.fetch.harvester import Harvester, BatchProtocol, RecordProtocol


class HarvestTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.log = StringIO()
        self.observer = CallTrace('observer')
        self._prepareHarvester()

    def _prepareHarvester(self, deleteAll=False):
        self.harvester = Harvester(self.tempdir, log=self.log, deleteAll=deleteAll)
        self.harvester._state.now = lambda: ZuluTime("1976-11-08T12:34:56Z")
        self.harvester.addObserver(self.observer)
        return self.harvester

    def testHarvestNoRecords(self):
        batch = Batch()
        batch.harvestingReady = True
        self.observer.returnValues['downloadBatch'] = batch
        self.harvester.harvest()
        self.assertEquals(['downloadBatch'], self.observer.calledMethodNames())
        self.assertEqual('Harvesting.\n0 added, 0 deleted, 0 unchanged, 0 skipped.\n-\nFinished harvesting.\n', self.log.getvalue())

    def testHarvestMoreThanOneBatch(self):
        self.harvester._events.markHarvestStart()
        self.harvester._events.markEvent(identifier='id0', uploadData='data0')
        self.harvester._events.markEvent(identifier='id1', uploadData='data1')
        self.harvester._events.markEvent(identifier='id9', uploadData='data9')
        self.harvester._events.markHarvestReady()
        self.assertEquals(['id0', 'id1', 'id9'], list(self.harvester._events.remainingAdds()))
        batches = []
        batch = Batch()
        batch.records = [Record('id0', 'data0'), Record('id1', 'data1')]
        batch.resumptionAttributes = lambda: {'key': 'value1'}
        batches.append(batch)
        batch = Batch()
        batch.records = [Record('id2', 'data2')]
        batch.harvestingReady = True
        batches.append(batch)
        self.observer.methods['downloadBatch'] = lambda **kwargs: batches.pop(0)
        self.observer.methods['convert'] =lambda record: 'converted.' + record.data
        self.harvester.harvest()
        self.assertEquals(['downloadBatch', 'convert', 'uploadRecord', 'convert', 'uploadRecord', 'downloadBatch', 'convert', 'uploadRecord', 'deleteRecord'], self.observer.calledMethodNames())
        lastDownloadBatchCall = self.observer.calledMethods[-4]
        self.assertEquals({'resumptionAttributes': {'key': 'value1'}}, lastDownloadBatchCall.kwargs)
        lastUploadRecordCall = self.observer.calledMethods[-2]
        self.assertEquals({'identifier': 'id2', 'data': 'converted.data2'}, lastUploadRecordCall.kwargs)
        deleteRecordCall = self.observer.calledMethods[-1]
        self.assertEquals({'identifier': 'id9'}, deleteRecordCall.kwargs)
        self.assertEquals(['id0', 'id1', 'id2'], list(self.harvester._events.remainingAdds()))

    def testDeleteAll(self):
        self.harvester._events.markHarvestStart()
        self.harvester._events.markEvent(identifier='id0', uploadData='data0')
        self.harvester._events.markEvent(identifier='id1', uploadData='data1')
        self.harvester._events.markEvent(identifier='id9', uploadData='data9')
        self.harvester._events.markHarvestReady()
        self.assertEquals(['id0', 'id1', 'id9'], list(self.harvester._events.remainingAdds()))
        self._prepareHarvester(deleteAll=True).harvest()
        self.assertEquals(['deleteRecord'] * 3, self.observer.calledMethodNames())
        self.assertEquals([], list(self.harvester._events.remainingAdds()))

    def testDownloadError(self):
        def downloadBatchRaises(resumptionAttributes):
            raise IOError('help!')
        self.observer.methods['downloadBatch'] = downloadBatchRaises
        try:
            self.harvester.harvest()
            self.fail()
        except IOError:
            pass
        persistedState = jsonLoad(open(join(self.tempdir, 'state')))
        self.assertEquals({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': None,
            'error': True}, persistedState)
        lastError = open(join(self.tempdir, 'last_error')).read()
        self.assertTrue('help!' in lastError, lastError)

    def testConvertError(self):
        batch = Batch()
        batch.records = [Record('id0', 'data0'), Record('id1', 'data1')]
        batch.harvestingReady = True
        self.observer.methods['downloadBatch'] = lambda **kwargs: batch
        def convertRaises(record):
            raise RuntimeError('help!')
        self.observer.methods['convert'] = convertRaises
        try:
            self.harvester.harvest()
            self.fail()
        except RuntimeError:
            pass
        persistedState = jsonLoad(open(join(self.tempdir, 'state')))
        self.assertEquals({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': None,
            'error': True}, persistedState)
        lastError = open(join(self.tempdir, 'last_error')).read()
        self.assertTrue('help!' in lastError, lastError)

    def testUploadError(self):
        batch = Batch()
        batch.records = [Record('id0', 'data0'), Record('id1', 'data1')]
        batch.harvestingReady = True
        self.observer.methods['downloadBatch'] = lambda **kwargs: batch
        def uploadRecordRaises(identifier, data):
            raise RuntimeError('help!')
        self.observer.methods['uploadRecord'] = uploadRecordRaises
        self.observer.returnValues['convert'] = 'converted'
        try:
            self.harvester.harvest()
            self.fail()
        except RuntimeError:
            pass
        persistedState = jsonLoad(open(join(self.tempdir, 'state')))
        self.assertEquals({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': None,
            'error': True}, persistedState)
        lastError = open(join(self.tempdir, 'last_error')).read()
        self.assertTrue('help!' in lastError, lastError)

    def testOnlyUploadUpdates(self):
        self.harvester._events.markHarvestStart()
        self.harvester._events.markEvent(identifier='id0', uploadData='converted.data0')
        self.harvester._events.markEvent(identifier='id1', uploadData='converted.data1')
        self.harvester._events.markHarvestReady()
        batch = Batch()
        batch.records = [Record('id0', 'data0'), Record('id1', 'data1.changed')]
        batch.harvestingReady = True
        self.observer.methods['downloadBatch'] = lambda **kwargs: batch
        self.observer.methods['convert'] =lambda record: 'converted.' + record.data
        self.harvester.harvest()
        self.assertEquals(['downloadBatch', 'convert', 'convert', 'uploadRecord'], self.observer.calledMethodNames())
        self.assertEquals({'identifier': 'id1', 'data': 'converted.data1.changed'}, self.observer.calledMethods[-1].kwargs)
        self.assertEquals(['id0', 'id1'], list(self.harvester._events.remainingAdds()))

    def testDeleteOnlyWhenNotAlready(self):
        self.harvester._events.markHarvestStart()
        self.harvester._events.markEvent(identifier='id0', uploadData='converted.data0')
        self.harvester._events.markEvent(identifier='id1', delete=True)
        self.harvester._events.markHarvestReady()
        batch = Batch()
        batch.records = [Record('id0', delete=True), Record('id1', delete=True)]
        batch.harvestingReady = True
        self.observer.methods['downloadBatch'] = lambda **kwargs: batch
        self.harvester.harvest()
        self.assertEquals(['downloadBatch', 'deleteRecord'], self.observer.calledMethodNames())
        self.assertEquals({'identifier': 'id0'}, self.observer.calledMethods[-1].kwargs)
        self.assertEquals([], list(self.harvester._events.remainingAdds()))


class Batch(BatchProtocol):
    def __init__(self):
        self.records = []
        self.harvestingReady = False

    def resumptionAttributes(self):
        return {}

class Record(RecordProtocol):
    def __init__(self, identifier, data=None, delete=False):
        self.identifier = identifier
        self.data = data
        self.delete = delete

    def mustAdd(self):
        return not self.delete

    def mustDelete(self):
        return self.delete

    def asString(self):
        return "%s: %s" % (self.identifier, self.data)

    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, repr(self.identifier), repr(self.data))

