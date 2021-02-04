## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015-2016, 2019 Seecr (Seek You Too B.V.) http://seecr.nl
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

from os.path import join, isfile
from io import StringIO

from simplejson import load as jsonLoad

from seecr.test import SeecrTestCase, CallTrace
from seecr.zulutime import ZuluTime

from meresco.components.json import JsonDict
from meresco.fetch.harvester import Harvester, BatchProtocol, RecordProtocol, SkipRecordException


class HarvestTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.log = StringIO()
        self.observer = CallTrace('observer', methods={'convert': (lambda record: str(record))})
        self._prepareHarvester()

    def _prepareHarvester(self, deleteAll=False):
        self.harvester = Harvester(self.tempdir, log=self.log, deleteAll=deleteAll, errorInterval=0.5)
        self.harvester._state.now = lambda: ZuluTime("1976-11-08T12:34:56Z")
        self.harvester.addObserver(self.observer)
        return self.harvester

    def _lastError(self):
        with open(join(self.tempdir, 'last_error')) as fp:
            return fp.read()
    def _state(self):
        with open(join(self.tempdir, 'state')) as fp:
            return jsonLoad(fp)

    def testHarvestNoRecords(self):
        batch = Batch()
        batch.harvestingReady = True
        self.observer.returnValues['downloadBatch'] = batch
        self.harvester.harvest()
        self.assertEqual(['downloadBatch', 'batchDone'], self.observer.calledMethodNames())
        self.assertEqual('Harvesting.\n0 added, 0 deleted, 0 unchanged, 0 skipped.\n-\nFinished harvesting.\n', self.log.getvalue())

    def testHarvestMoreThanOneBatch(self):
        self.harvester._events.markHarvestStart()
        self.harvester._events.markEvent(identifier='id0', uploadData='data0')
        self.harvester._events.markEvent(identifier='id1', uploadData='data1')
        self.harvester._events.markEvent(identifier='id9', uploadData='data9')
        self.harvester._events.markHarvestReady()
        self.assertEqual(['id0', 'id1', 'id9'], list(self.harvester._events.remainingAdds()))
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
        self.assertEqual(['downloadBatch', 'convert', 'uploadRecord', 'convert', 'uploadRecord', 'batchDone', 'downloadBatch', 'convert', 'uploadRecord', 'batchDone', 'deleteRecord'], self.observer.calledMethodNames())
        lastDownloadBatchCall = self.observer.calledMethods[-5]
        self.assertEqual({'resumptionAttributes': {'key': 'value1'}}, lastDownloadBatchCall.kwargs)
        lastUploadRecordCall = self.observer.calledMethods[-3]
        self.assertEqual({'identifier': 'id2', 'data': 'converted.data2'}, lastUploadRecordCall.kwargs)
        deleteRecordCall = self.observer.calledMethods[-1]
        self.assertEqual({'identifier': 'id9'}, deleteRecordCall.kwargs)
        self.assertEqual(['id0', 'id1', 'id2'], list(self.harvester._events.remainingAdds()))

    def testDeleteAll(self):
        self.harvester._events.markHarvestStart()
        self.harvester._events.markEvent(identifier='id0', uploadData='data0')
        self.harvester._events.markEvent(identifier='id1', uploadData='data1')
        self.harvester._events.markEvent(identifier='id9', uploadData='data9')
        self.harvester._events.markHarvestReady()
        self.assertEqual(['id0', 'id1', 'id9'], list(self.harvester._events.remainingAdds()))
        self._prepareHarvester(deleteAll=True).harvest()
        self.assertEqual(['deleteRecord'] * 3, self.observer.calledMethodNames())
        self.assertEqual([], list(self.harvester._events.remainingAdds()))

    def testDownloadError(self):
        def downloadBatchRaises(resumptionAttributes):
            raise IOError('help!')
        self.observer.methods['downloadBatch'] = downloadBatchRaises
        try:
            self.harvester.harvest()
            self.fail()
        except IOError:
            pass
        persistedState = self._state()
        self.assertEqual({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': None,
            'error': True}, persistedState)

        lastError = self._lastError()
        self.assertTrue('help!' in lastError, lastError)

        # and test that it's cleaned up after first succesful batch is processed
        def downloadBatch(resumptionAttributes):
            batch = Batch()
            batch.records = [Record('identifier0', 'data0')]
            batch.quitForSleep = True
            return batch
        self.observer.methods['downloadBatch'] = downloadBatch
        self.harvester.harvest()
        persistedState = self._state()
        self.assertEqual({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': {},
            'error': False}, persistedState)
        self.assertFalse(isfile(join(self.tempdir, 'last_error')))

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
        persistedState = self._state()
        self.assertEqual({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': None,
            'error': True}, persistedState)
        lastError = self._lastError()
        self.assertTrue('help!' in lastError, lastError)

    def testSkipRecordException(self):
        batch = Batch()
        batch.records = [Record('id0', 'data0'), Record('id1', 'data1')]
        batch.harvestingReady = True
        self.observer.methods['downloadBatch'] = lambda **kwargs: batch
        def convertRaises(record):
            raise SkipRecordException()
        self.observer.methods['convert'] = convertRaises
        self.harvester.harvest()
        self.assertEqual([
            'Harvesting.',
            "Skipping record 'id0'",
            "Skipping record 'id1'",
            "0 added, 0 deleted, 0 unchanged, 2 skipped.",
            "-",
            "Finished harvesting.",
            ""], self.log.getvalue().split("\n"))


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
        persistedState = self._state()
        self.assertEqual({
            'harvestingReady': False,
            'datetime': '1976-11-08T12:34:56Z',
            'resumptionAttributes': None,
            'error': True}, persistedState)
        lastError = self._lastError()
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
        self.assertEqual(['downloadBatch', 'convert', 'convert', 'uploadRecord', 'batchDone'], self.observer.calledMethodNames())
        self.assertEqual({'identifier': 'id1', 'data': 'converted.data1.changed'}, self.observer.calledMethods[-2].kwargs)
        self.assertEqual(['id0', 'id1'], list(self.harvester._events.remainingAdds()))

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
        self.assertEqual(['downloadBatch', 'deleteRecord', 'batchDone'], self.observer.calledMethodNames())
        self.assertEqual({'identifier': 'id0'}, self.observer.calledMethods[-2].kwargs)
        self.assertEqual([], list(self.harvester._events.remainingAdds()))

    def testDeleteOldIfHarvestingReady(self):
        JsonDict({
                'harvestingReady': True,
                'datetime': '1976-11-08T12:34:56Z',
                'resumptionAttributes': None,
                'error': False
            }).dump(join(self.tempdir, 'state'))
        with open(join(self.tempdir, 'current'), 'w') as fp:
            fp.write("")
        with open(join(self.tempdir, 'previous'), 'w') as f:
            f.write("id:1\tA\tdatahash\n")
            f.write("id:2\tA\tdatahash\n")
        self._prepareHarvester()
        self.harvester.harvest()
        self.assertEqual(['deleteRecord', 'deleteRecord'], self.observer.calledMethodNames())
        self.assertEqual({'identifier': 'id:1'}, self.observer.calledMethods[0].kwargs)
        self.assertEqual({'identifier': 'id:2'}, self.observer.calledMethods[1].kwargs)

    def testQuitForSleep(self):
        batch = Batch()
        batch.harvestingReady = False
        batch.quitForSleep = True
        self.observer.returnValues['downloadBatch'] = batch
        self.harvester.harvest()
        self.assertEqual(['downloadBatch', 'batchDone'], self.observer.calledMethodNames())
        # Note: previous line implicitly asserts that deleteRecord was not invoked, as _deleteOldRecords should not be executed when only quiting for sleep.
        self.assertEqual('Harvesting.\n0 added, 0 deleted, 0 unchanged, 0 skipped.\n-\nQuiting for sleep.\n', self.log.getvalue())


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
