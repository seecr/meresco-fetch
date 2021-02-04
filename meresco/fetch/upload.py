## begin license ##
#
# "Meresco Fetch" is a small framework to build simple, custom harvesters.
#
# Copyright (C) 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from urllib.parse import urlsplit

from weightless.core import consume
from meresco.components.sru import SruUpdateClient


class Upload(object):
    def __init__(self, baseUrl, log, userAgent="Meresco-Fetch Harvester"):
        if not baseUrl:
            self._sruUpdateClient = _Ignore()
        else:
            _, netloc, path, _, _ = urlsplit(baseUrl)
            host, port = netloc.split(':', 1) if ':' in netloc else (netloc, 80)
            self._sruUpdateClient = SruUpdateClient(
                host=host,
                port=int(port),
                path=path,
                userAgent=userAgent,
                synchronous=True)
        self._log = log
        self._log.write("Uploading to: %s\n" % repr(baseUrl))

    def uploadRecord(self, identifier, data):
        self._upload(identifier, data)
        self._log.write("Uploaded: %s\n" % identifier)

    def deleteRecord(self, identifier):
        self._delete(identifier)
        self._log.write("Deleted: %s\n" % identifier)

    def _upload(self, identifier, data):
        consume(self._sruUpdateClient.add(identifier=identifier, data=data))

    def _delete(self, identifier):
        consume(self._sruUpdateClient.delete(identifier=identifier))


class _Ignore(object):
    def add(self, **kwargs):
        return
        yield

    def delete(self, **kwargs):
        return
        yield
