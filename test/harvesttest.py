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

