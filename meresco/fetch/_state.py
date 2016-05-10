## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
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

from os.path import isfile
from seecr.zulutime import ZuluTime
from meresco.components.json import JsonDict

class State(object):
    def __init__(self, filePath):
        self._filePath = filePath
        self.clear()

    def clear(self):
        self.datetime = None
        self.harvestingReady = False
        self.error = None
        self.resumptionAttributes = None
        return self

    @classmethod
    def load(cls, filePath):
        state = cls(filePath=filePath)
        if isfile(filePath):
            d = JsonDict.load(filePath)
            state.datetime = d.get('datetime')
            state.harvestingReady = d.get('harvestingReady', False)
            state.error = d.get('error')
            state.resumptionAttributes = d.get('resumptionAttributes')
        return state

    def save(self):
        self.datetime = self.now().zulu()
        JsonDict(
            datetime=self.datetime,
            harvestingReady=self.harvestingReady,
            error=self.error,
            resumptionAttributes=self.resumptionAttributes
        ).dump(self._filePath)

    def now(self):
        return ZuluTime()

