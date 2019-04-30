from logging import getLogger
logger = getLogger(__name__)

from threading import Thread
import json
import time

SUTATUSES = ["IN_PROGRESS", "FAILED", "SUCCEEDED", "REJECTED"]

class JobScenario:
    client = None
    thingName = ''

    def __init__(self, jobId, queuedAt, lastUpdatedAt, executionNumber, versionNumber, jobDocument, startedAt=None, client=None, thingName=thingName):
        self.jobId = jobId
        self.queuedAt = queuedAt
        self.lastUpdatedAt = lastUpdatedAt
        self.executionNumber = executionNumber
        self.versionNumber = versionNumber
        self.startedAt = startedAt
        self.client = client
        self.thingName = thingName
        self.jobDocument = jobDocument

    def _exec(self):
        logger.debug("start job scenario.")
        try:
            r = self.exec(self.jobDocument)
            if r:
                self.changeStatus('SUCCEEDED', {"reason": "progress success."})
        except Exception as e:
            logger.exception(e)
            self.changeStatus('FAILED', {"reason": "execution failed."})

    def _throw_job(self):
        logger.debug('_throw_job function.')
        d = Thread(target=self._exec, daemon=False)
        d.start()
        logger.debug('throwed.')
        return d

    @staticmethod
    def valid(self, jobDocument):
        return False

    def changeStatus(self, status='PROGRESS', details={}):
        if status not in SUTATUSES:
            return

        self.status = status
        self._jobs_update(self.versionNumber, None, self.status, details)
        self.versionNumber += 1
        time.sleep(1)

    def _jobs_update(self, expectedVersion, clientToken, status='IN_PROGRESS', details={}):
        topic = '$aws/things/{0}/jobs/{1}/update'.format(self.thingName, self.jobId)
        report = {
          "status": status,
          "statusDetails": details,
          "expectedVersion": expectedVersion,
          "clientToken": clientToken
        }
        logger.debug('publish %s %s', topic, json.dumps(report))
        self.client.publish(topic, json.dumps(report))
        time.sleep(1)
