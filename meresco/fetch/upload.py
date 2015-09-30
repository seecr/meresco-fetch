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

from meresco.components import lxmltostring
from meresco.components.sru import SruUpdateClient
from weightless.core import compose

from urlparse import urlsplit

class Upload(object):
    def __init__(self, baseUrl, log, userAgent="Zoekplatform Harvester"):
        _, netloc, path, _, _ = urlsplit(baseUrl)
        host, port = netloc.split(':', 1) if ':' in netloc else (netloc, 80)
        self._log = log
        self._sruUpdateClient = SruUpdateClient(
            host=host,
            port=int(port),
            path=path,
            userAgent=userAgent,
            synchronous=True)
        self._log.write("Uploading to: %s\n" % baseUrl)

    def _upload(self, identifier, rdf):
        list(compose(self._sruUpdateClient.add(identifier=identifier, data=lxmltostring(rdf))))

    def _delete(self, identifier):
        list(compose(self._sruUpdateClient.delete(identifier=identifier)))

    def uploadRecord(self, identifier, rdf):
        self._upload(identifier, rdf)
        self._log.write("Uploaded: %s\n" % identifier)

    def deleteRecord(self, identifier):
        self._delete(identifier)
        self._log.write("Deleted: %s\n" % identifier)

