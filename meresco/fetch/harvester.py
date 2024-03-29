## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2014-2016, 2019, 2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2021 SURF https://www.surf.nl
# Copyright (C) 2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from os import rename, makedirs, remove
from os.path import join, isdir, isfile
from sys import exc_info
from traceback import print_exception
from time import sleep
from hashlib import md5

from seecr.zulutime import ZuluTime

from meresco.core import Observable

from ._state import State


class SkipRecordException(Exception):
    pass


class BatchProtocol(object):
    def __init__(self):
        self.records = []
        self.harvestingReady = False
        self.quitForSleep = False

    def resumptionAttributes(self):
        return 'a structure that can be saved as value in the JSON state file, signifying how next batch can be retrieved'


class RecordProtocol(object):
    def __init__(self):
        self.identifier = 'identifier used with SRU update'

    def mustAdd(self):
        return False

    def mustDelete(self):
        return False

    def asString(self):
        return 'record string representation for debugging'


class Harvester(Observable):
    def __init__(self, statePath, log=None, name=None, deleteAll=False, harvestInterval=24*60*60, errorInterval=10):
        Observable.__init__(self, name=name)
        self._statePath = statePath
        if not isdir(statePath):
            makedirs(statePath)
        self._state = State.load(filePath=join(self._statePath, 'state'))
        self._events = _Events(self._statePath)
        self._logWrite = (lambda aString: None) if log is None else log.write
        self._deleteAll = deleteAll
        self._harvestInterval = harvestInterval
        self._errorInterval = errorInterval

    def harvest(self):
        self._waitAWhileAfterError()

        if self._deleteAll:
            return self._deleteAllRecords()
        if self._state.harvestingReady:
            self._deleteOldRecords()  # possibly still needs to be finished after crash
            if self._harvestIntervalElapsed():
                self._state.clear()
                self._state.save()
            else:
                self._logWrite('Harvesting ready since {0}.\n'.format(self._state.datetime))
                self._logWrite('Waiting until {0} seconds have passed.\n'.format(self._harvestInterval))
                return

        self._logWrite('Harvesting.\n')
        self._events.markHarvestStart()
        while not self._state.harvestingReady:
            try:
                batch = self.call.downloadBatch(resumptionAttributes=self._state.resumptionAttributes or dict())
                self._processBatch(batch)
                if getattr(batch, 'quitForSleep', False):
                    self._logWrite('Quiting for sleep.\n')
                    return
            except (SystemExit, KeyboardInterrupt, AssertionError):
                raise
            except Exception:
                self._saveError()
                raise

        self._deleteOldRecords()
        self._events.markHarvestReady()
        self._logWrite('Finished harvesting.\n')

    def _processBatch(self, batch):
        added = deleted = unchanged = skipped = 0
        for record in batch.records:
            if record.mustDelete():
                if self._events.alreadyDeleted(record.identifier):
                    unchanged += 1
                else:
                    deleted += 1
                    self.do.deleteRecord(identifier=record.identifier)
                self._events.markEvent(record.identifier, delete=True)
            elif record.mustAdd():
                try:
                    uploadData = self.call.convert(record=record)
                    if self._events.alreadyAdded(record.identifier, uploadData):
                        unchanged += 1
                    else:
                        self.call.uploadRecord(identifier=record.identifier, data=uploadData)
                        added += 1
                    self._events.markEvent(record.identifier, uploadData)
                except SkipRecordException:
                    skipped += 1
                    self._logWrite("Skipping record '%s'\n" % record.identifier)
            else:
                skipped += 1
                self._logWrite("Skipping record '%s'\n" % record.identifier)
        self._logWrite("%d added, %d deleted, %d unchanged, %d skipped.\n-\n" % (added, deleted, unchanged, skipped))
        self._state.resumptionAttributes = batch.resumptionAttributes()
        self._state.harvestingReady = batch.harvestingReady
        self._state.error = False
        self._state.save()
        self._clearError()
        self.do.batchDone(batch)

    def _deleteOldRecords(self):
        for identifier in self._events.toBeDeleted():
            self.do.deleteRecord(identifier=identifier)
            self._events.markEvent(identifier, delete=True)

    def _deleteAllRecords(self):
        self._events.markHarvestStart()
        for identifier in self._events.remainingAdds():
            self.do.deleteRecord(identifier=identifier)
            self._events.markEvent(identifier, delete=True)
        self._state.clear()
        self._state.error = False
        self._state.save()
        self._clearError()
        self._events.markHarvestReady()

    def _harvestIntervalElapsed(self):
        return self._state.now().epoch - ZuluTime(self._state.datetime).epoch > self._harvestInterval

    def _saveError(self, record=None):
        self._state.error = True
        self._state.save()
        exc_type, exc_value, exc_traceback = exc_info()
        with open(join(self._statePath, "last_error"), "w") as fp:
            print_exception(exc_type, exc_value, exc_traceback, file=fp)
        if not record is None:
            with open(join(self._statePath, "last_error.record"), 'w') as fp:
                fp.write(record.asString())

    def _clearError(self):
        lastErrorF = join(self._statePath, "last_error")
        lastErrorRecordF = join(self._statePath, "last_error.record")
        for f in [lastErrorF, lastErrorRecordF]:
            if isfile(f):
                remove(f)

    def _waitAWhileAfterError(self):
        if self._state.error:
            self._logWrite('Harvesting in error state since {0}: {1}.\n'.format(self._state.datetime, self._lastError()))
            waitTime = ZuluTime(self._state.datetime).epoch + self._errorInterval - self._state.now().epoch
            if waitTime > 0:
                self._logWrite('Waiting until {0} seconds have passed.\n'.format(self._errorInterval))
                sleep(waitTime)  # Note!!

    def _lastError(self):
        with open(join(self._statePath, "last_error")) as fp:
            return fp.read().strip()


class _Events(object):
    def __init__(self, stateDir):
        self._currentEventsPath = join(stateDir, 'current')
        self._previousEventsPath = join(stateDir, 'previous')
        self._readPrevious()
        self._harvestStarted = False

    def markEvent(self, identifier, uploadData=None, delete=False):
        dataHash = '' if delete else self._makeHash(uploadData)
        with open(self._currentEventsPath, 'a') as fp:
            fp.write("%s\t%s\t%s\n" % (identifier, ('D' if delete else 'A'), dataHash))

    def alreadyDeleted(self, identifier):
        action = self._previous.get(identifier)
        return action[0] == 'D' if action else False

    def alreadyAdded(self, identifier, uploadData):
        previousHash = None
        action = self._previous.get(identifier)
        if action:
            if action[0] != 'A':
                return False
            previousHash = action[1]
        return previousHash == self._makeHash(uploadData)

    def toBeDeleted(self):
        if not isfile(self._currentEventsPath):
            return
        currentIdentifiers = set(self._readEvents(self._currentEventsPath).keys())
        for identifier, action in sorted(self._previous.items()):
            if identifier in currentIdentifiers:
                continue
            if action[0] != 'D':
                yield identifier

    def remainingAdds(self):
        current = self._readEvents(self._currentEventsPath)
        for identifier, action in (sorted(self._previous.items()) + sorted(current.items())):
            if action[0] == 'A':
                currentIdState = current.get(identifier)
                if currentIdState and currentIdState[0] == 'D':
                    continue
                yield identifier

    def markHarvestStart(self):
        if not isfile(self._currentEventsPath):
            open(self._currentEventsPath, 'w').close()
        self._harvestStarted = True

    def markHarvestReady(self):
        assert self._harvestStarted
        rename(self._currentEventsPath, self._previousEventsPath)
        self._readPrevious()
        self._harvestStarted = False

    def _makeHash(self, data):
        return md5(data.encode()).hexdigest()

    def _readPrevious(self):
        self._previous = self._readEvents(self._previousEventsPath)

    def _readEvents(self, eventsFilePath):
        if not isfile(eventsFilePath):
            return {}
        with open(eventsFilePath) as fp:
            return dict(
                headTail(split(line.strip(), '\t', 3))
                for line in fp.read().strip().split("\n")
                if line.strip()
            )


def split(line, separator, expectedNumber):
    r = line.split(separator)
    return r + (expectedNumber - len(r)) * [None]

def headTail(s):
    return (s[0], s[1:])
