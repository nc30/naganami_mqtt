# coding: utf-8
from logging import getLogger
logger = getLogger(__name__)

import re
import os
import json
import time
from .mqtt_base import MqttController

class AwsIotDeviceCredential:
    caPath = None
    certPath = None
    keyPath = None
    thingName = None
    iotHost = None
    port = 8883

    def __init__(self,
            thingName, caPath, keyPath, certPath, iotHost, port=8883):
        self.thingName = thingName
        self.caPath = caPath
        self.keyPath = keyPath
        self.certPath = certPath
        self.iotHost = iotHost
        self.port = port

    def certExist(self):
        return os.path.exists(self.caPath) and os.path.exists(self.keyPath) and os.path.exists(self.certPath)

def getAwsCredentialFromJson(jsonPath):
    with open(jsonPath, 'r') as f:
        conf = json.loads(f.read())

    return AwsIotDeviceCredential(
        conf['thingName'], conf['caPath'], conf['keyPath'], conf['certPath'], conf['iotHost']
    )


class AwsIotContoller(MqttController):
    jobScenarios = []

    def __init__(self, credenchals, connect=True, *args, **kwargs):
        self.credenchals = credenchals

        super(AwsIotContoller, self).__init__(
            name=self.credenchals.thingName,
            host=self.credenchals.iotHost,
            port=self.credenchals.port,
            ca_certs=self.credenchals.caPath,
            certfile=self.credenchals.certPath,
            keyfile=self.credenchals.keyPath,
            rwt_use=True,
            rwt_retain=False,
            connect=connect,
            *args, **kwargs
        )

    def _set_subscribe_topics(self, client, userdata, flags, respons_code):
        self.logger.debug('_set_subscribe_topics function')
        client.subscribe('$aws/things/{0}/shadow/update/delta'.format(self.name))
        client.subscribe('$aws/things/{0}/jobs/+/get/accepted'.format(self.name))
        client.subscribe('$aws/things/{0}/jobs/$next/get/accepted'.format(self.name))
        client.subscribe('$aws/things/{0}/jobs/notify-next'.format(self.name))
        super(AwsIotContoller, self)._set_subscribe_topics(client, userdata, flags, respons_code)

    def _parse_topics(self, client, userdata, msg):
        try:
            if msg.topic == '$aws/things/{0}/shadow/update/delta'.format(self.name):
                self._delta_function(client, userdata, msg)
                return None
            if msg.topic == '$aws/things/{0}/jobs/notify-next'.format(self.name):
                self._jobs_get_notify_next(client, userdata, msg)
                return None
            if msg.topic == '$aws/things/{0}/jobs/get/accepted'.format(self.name):
                self._jobs_get_accepted(client, userdata, msg)
                return None

            r = re.match(r'^\$aws/things/([^/\+#]+)/jobs/([^/\+#]+)/get/accepted$', msg.topic)
            if r and r.groups()[0] == self.name:
                self._jobs_get_notify_next(client, userdata, msg)
                return None

        except Exception as e:
            self.logger.exception(e)

        super(AwsIotContoller, self)._parse_topics(client, userdata, msg)

    def _jobs_get_accepted(self, client, userdata, msg):
        self.logger.debug('get job accepted.')
        payload = json.loads(msg.payload.decode())
        self.logger.debug('payload: %s', payload)

        checkJob = None
        if len(payload.get('inProgressJobs', [])) > 0:
            checkJob = payload['inProgressJobs'][0]['jobId']

        if checkJob is None and len(payload.get('queuedJobs', [])) > 0:
            checkJob = payload['queuedJobs'][0]['jobId']

        self.logger.debug('checkjob is %s', checkJob)
        if checkJob:
            topic = '$aws/things/{0}/jobs/{1}/get'.format(self.name, checkJob)
            self.publish(topic, json.dumps({
                "executionNumber": 1,
                "includeJobDocument": True,
                "clientToken": ""
            }))

    def _jobs_get_notify_next(self, client, userdata, msg):
        self.logger.debug('get notfy-next.')
        payload = json.loads(msg.payload.decode())
        self.logger.debug('payload: %s', payload)

        job = payload.get("execution", None)
        if job is None:
            return

        jobSchenario = self._job_check(job)

        if jobSchenario:
            self.currentJob = jobSchenario._throw_job()
        else:
            self.job_update(job['jobId'], job['versionNumber'], None, 'REJECTED', {"reason": "jobScenario not found."})
            return

    def job_update(self, jobId, expectedVersion, clientToken, status='IN_PROGRESS', details={}):
        topic = '$aws/things/{0}/jobs/{1}/update'.format(self.name, jobId)
        report = {
          "status": status,
          "statusDetails": details,
          "expectedVersion": expectedVersion,
          "clientToken": clientToken
        }
        self.publish(topic, json.dumps(report))

    def _job_check(self, job):
        for scenario in self.jobScenarios:
            if scenario.valid(job['jobDocument']):
                return scenario(
                    jobId=job['jobId'],
                    status=job['status'],
                    controller=self,
                    queuedAt=job['queuedAt'],
                    lastUpdatedAt=job['lastUpdatedAt'],
                    executionNumber=job['executionNumber'],
                    versionNumber=job['versionNumber'],
                    jobDocument=job['jobDocument'],
                    thingName=self.name
                )
        return False

    def request_job(self):
        self.publish('$aws/things/{0}/jobs/get'.format(self.name), '')

    def _shadow_update(self, reported={}, desired={}):
        payload = {
            "state": {}
        }
        if reported:
            payload['state']['reported'] = reported

        if desired:
            payload['state']['desired'] = desired

        self.logger.debug('shadow update %s', payload)
        self.client.publish('$aws/things/'+self.name+'/shadow/update', json.dumps(payload), 1)

    def _delta_function(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            self.logger.debug('delta function %s', payload)
            self.delta_function(payload)
        except Exception as e:
            self.logger.exception(e)

    def delta_function(self, payload):
        pass

