# coding: utf-8
from logging import getLogger, NullHandler
logger = getLogger(__name__)
logger.addHandler(NullHandler())

import os
import json
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
    def __init__(self, credenchals, connect=True, *args, **kwargs):
        self.credenchals = credenchals

        super(AwsIotContoller, self).__init__(name=self.credenchals.thingName,
            host=self.credenchals.iotHost, port=self.credenchals.port,
            ca_certs=self.credenchals.caPath, certfile=self.credenchals.certPath, keyfile=self.credenchals.keyPath,
            rwt_use=True, rwt_retain=False, connect=connect,
            *args, **kwargs
        )

    def _on_connect(self, client, userdata, flags, respons_code):
        client.subscribe('$aws/things/{0}/shadow/update/delta'.format(self.name))

        super(AwsIotContoller, self)._on_connect(client, userdata, flags, respons_code)


    def _on_message(self, client, userdata, msg):
        if msg.topic == '$aws/things/{0}/shadow/update/delta'.format(self.name):
            self._delta_function(client, userdata, msg)
            return

        super(AwsIotContoller, self)._on_message(client, userdata, msg)

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
