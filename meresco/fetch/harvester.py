## begin license ##
#
# "NBC+" also known as "ZP (ZoekPlatform)" is
#  a project of the Koninklijke Bibliotheek
#  and provides a search service for all public
#  libraries in the Netherlands.
#
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
#
# This file is part of "NBC+ (Zoekplatform BNL)"
#
# "NBC+ (Zoekplatform BNL)" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "NBC+ (Zoekplatform BNL)" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "NBC+ (Zoekplatform BNL)"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from meresco.core import Observable

from simplejson import load as jsonLoad, dump as jsonSave
from os import rename, makedirs
from os.path import join, isdir, isfile
from sys import exc_info
from traceback import print_exception

from seecr.zulutime import ZuluTime

class Harvester(Observable):
    def __init__(self, statePath, log=None, name=None, deleteAll=False, harvestInterval=24*60*60):
        Observable.__init__(self, name=name)
        self._statePath = statePath
        if not isdir(statePath):
            makedirs(statePath)
        self._logWrite = (lambda aString: None) if log is None else log.write
        self._deleteAll = deleteAll
        self._harvestInterval = harvestInterval

    def harvest(self):
        if self._deleteAll:
            return self._deleteAllRecords()
        currentState = self._loadState()
        today = self._today()
        if currentState['state'] != "busy":
            if not shouldRetryHarvest(currentState['date'], self._now(), self._harvestInterval):
                self._logWrite('Harvesting already done in last {} seconds.\n'.format(self._harvestInterval))
                return
            currentState = self._saveState(dict(state="busy"))

        self._logWrite("Harvesting records with current date: %s\n" % today)

        while True:
            batch = self.call.downloadBatch(current=today, state=currentState.get('download', dict()))
            added = deleted = skipped = 0
            for record in batch.records:
                if record.mustDelete():
                    deleted += 1
                    self.do.deleteRecord(record.identifier)
                elif record.mustAdd():
                    try:
                        rdf = self.call.convert(record=record)
                    except (SystemExit, KeyboardInterrupt, AssertionError):
                        raise
                    except Exception:
                        currentState['state'] = "error"
                        self._saveState(currentState)
                        self.logLastError(record)
                        raise
                    self.call.uploadRecord(record.identifier, rdf)
                    added += 1
                    self._logIdentifier(record.identifier)
                else:
                    skipped += 1
                    self._logWrite("Skipping record '%s'\n" % record.identifier)

            self._logWrite("%d added, %d deleted, %d skipped.\n-\n" % (added, deleted, skipped))
            currentState['download'] = batch.getState()
            self._saveState(currentState)

            if batch.isCompleted():
                currentState['state'] = "completed"
                self._saveState(currentState)
                self._deleteOldRecords()
                break

    def _deleteOldRecords(self):
        previousIdentifiersFile = join(self._statePath, "previous_uploaded_identifiers")
        if isfile(previousIdentifiersFile):
            previousIdentifiers = [identifier.strip() for identifier in open(previousIdentifiersFile).read().strip().split("\n") if identifier.strip()]
            currentIdentifiers = [identifier.strip() for identifier in open(self.identifiersFile()).read().strip().split("\n") if identifier.strip()]

            for identifier in previousIdentifiers:
                if identifier in currentIdentifiers:
                    continue
                self.do.deleteRecord(identifier)
        if isfile(self.identifiersFile()):
            rename(self.identifiersFile(), previousIdentifiersFile)

    def _deleteAllRecords(self):
        if not isfile(self.identifiersFile()):
            open(self.identifiersFile(), 'w').close()
        self._deleteOldRecords()

    def _filename(self):
        return join(self._statePath, "state")

    def _loadState(self):
        filename = self._filename()
        return jsonLoad(open(filename)) if isfile(filename) else dict(state="busy", date=None)

    def _saveState(self, state):
        state['date'] = self._now()
        filename = self._filename()
        jsonSave(state, open(filename + ".tmp", "w"))
        rename(filename + ".tmp", filename)
        return state

    def lastError(self):
        return open(join(self._statePath, "last_error")).read().strip()

    def logLastError(self, record):
        exc_type, exc_value, exc_traceback = exc_info()
        with open(join(self._statePath, "last_error"), "w") as fp:
            print_exception(exc_type, exc_value, exc_traceback, file=fp)
        open(join(self._statePath, "last_error.record"), 'w').write(record.asString(pretty_print=True))

    def _logIdentifier(self, identifier):
        open(self.identifiersFile(), 'a').write("%s\n" % identifier)

    def identifiersFile(self):
        return join(self._statePath, 'identifiers')

    def _now(self):
        return ZuluTime().zulu()

    def _today(self):
        return '{0}T00:00:00Z'.format(self._now().split('T')[0])

def shouldRetryHarvest(lhs, rhs, seconds):
    lhs, rhs = ZuluTime(lhs), ZuluTime(rhs)
    return rhs.epoch - lhs.epoch > seconds
