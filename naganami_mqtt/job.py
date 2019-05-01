from logging import getLogger
logger = getLogger(__name__)

from threading import Thread
import json
import time
import uuid
import re

SEQUENCE_REGEX = re.compile(r'^sequence(?P<number>[0-9]+)_(?P<name>.+)$')

SUTATUSES = ["IN_PROGRESS", "FAILED", "SUCCEEDED", "REJECTED"]
AWAITING = 'awaiting'
IN_PROGRESS = 'in-progress'
DONE = 'done'
FAILED = 'failed'

class CommunicationError(Exception):
    pass

class QueueTimeoutError(Exception):
    pass

class Harakiri(Exception):
    pass

class JobScenario:
    client = None
    thingName = ''

    def __init__(self, controller, jobId, status, queuedAt, lastUpdatedAt,
            executionNumber, versionNumber, jobDocument, statusDetails=None,
            startedAt=None, thingName=thingName):
        self.jobId = jobId
        self.status = status
        self.queuedAt = queuedAt
        self.lastUpdatedAt = lastUpdatedAt
        self.executionNumber = executionNumber
        self.versionNumber = versionNumber
        self.startedAt = startedAt
        self.controller = controller
        self.thingName = thingName
        self.jobDocument = jobDocument
        self.sequences = self._sequences()
        logger.info('sequences: %s', self.sequences)

        if statusDetails:
            self.statusDetails = statusDetails
        else:
            self.statusDetails = {}
            for s in self.sequences:
                self.statusDetails[s['name']] = AWAITING
        logger.info('status %s', self.statusDetails)

    def _sequences(self):
        sequences = []
        for att in self.__dir__():
            r = SEQUENCE_REGEX.match(att)
            if r:
                sequences.append(
                    (r.group('number'),
                    {
                        "name": r.group('name'),
                        "fnc": getattr(self, r.string)
                    })
                )
        return [s[1] for s in sorted(sequences)]

    def get_next_sequence(self):
        for k, v in self.sequences.items():
            if v == AWAITING:
                return self.v

    def _exec(self):
        for seq in self.sequences:
            if self.statusDetails.get(seq['name']) == AWAITING:
                sequence = seq['fnc']

                logger.info('do job "%s"', seq['name'])
                self.statusDetails[seq['name']] = IN_PROGRESS
                self.changeStatus(status='IN_PROGRESS')

                try:
                    sequence(self.jobDocument, self.statusDetails)
                    self.statusDetails[seq['name']] = DONE
                    self.changeStatus(status='IN_PROGRESS')

                except Harakiri:
                    logger.info('haiku wo yome.')
                    return

                except Exception as e:
                    logger.exception(e)
                    self.statusDetails[seq['name']] = FAILED
                    self.changeStatus(status='FAILED')
                    return False
        self.changeStatus(status='SUCCEEDED')
        logger.info('job secuence all done.')

    def _throw_job(self):
        d = Thread(target=self._exec, daemon=False)
        d.start()
        logger.debug('throwed.')
        return d

    def kill_my_self(self):
        raise Harakiri()

    def valid(self, jobDocument):
        return False

    def changeStatus(self, status='IN_PROGRESS'):
        if status not in SUTATUSES:
            raise KeyError(str(status) + ' is invalid status.')

        self.status = status
        for i in range(3):
            r = self._jobs_update(self.versionNumber, self.status, self.statusDetails)
            if r:
                self.versionNumber += 1
                return
            time.sleep(1)
        raise CommunicationError()

    def wait_topic(self, topics, func, timeout=5):
        self.wait_message = None
        self.controller.set_wait(topics, self.wait, timeout + 0.5)
        func()
        for i in range(timeout * 2):
            if self.wait_message:
                msg = self.wait_message
                self.wait_message = None
                return msg
            time.sleep(0.5)
        raise QueueTimeoutError('expected topic '+str(topics)+' is not received.')

    def wait(self, client, userdata, msg):
        self.wait_message = msg

    def _jobs_update(self, expectedVersion, status, details, force=False):
        clientToken = uuid.uuid4().urn
        def func():
            topic = '$aws/things/{0}/jobs/{1}/update'.format(self.thingName, self.jobId)
            report = {
              "status": status,
              "statusDetails": details,
              "expectedVersion": expectedVersion,
              "clientToken": clientToken
            }
            self.controller.publish(topic, json.dumps(report))

        if force:
            func()
            return

        msg = self.wait_topic(
            [
                '$aws/things/{0}/jobs/{1}/update/accepted'.format(self.thingName, self.jobId),
                '$aws/things/{0}/jobs/{1}/update/rejected'.format(self.thingName, self.jobId)
            ],
            func,
            timeout = 10
        )

        if json.loads(msg.payload.decode())['clientToken'] == clientToken:
            if msg.topic[-9:] == '/accepted':
                return True

        return False
