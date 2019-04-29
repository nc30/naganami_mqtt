from logging import getLogger
logger = getLogger(__name__)

from threading import Thread
import json
import time

SUTATUSES = ["IN_PROGRESS", "FAILED", "SUCCEEDED", "REJECTED"]

from functools import wraps
def executeWrapper(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f
        except Exception as e:
            logger.exception(e)
        return False
    return wrapper



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


    @executeWrapper
    def exec(self, jobDocument):
        pass

    def _throw_job(self):
        d = Thread(target=self.exec, args=(self.jobDocument,))
        d.start()

    @staticmethod
    def valid(self, jobDocument):
        return False

    def changeStatus(self, status='PROGRESS', details={}):
        if status not in SUTATUSES:
            return

        self.status = status
        self._jobs_update(self.versionNumber, None, self.status, details)
        self.versionNumber += 1

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


if __name__ == '__main__':
    class sample(JobScenario):
        def exec(self, jobDocument):
            logger.debug(self)
            logger.debug(document)
            raise TypeError()

    import time
    import logging
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    logger.debug("aaaaaaaaa")

    job = sample('testjob', 123, 123,123,123,{'ni':'naganami'})

    job._throw_job()

    time.sleep(10)
