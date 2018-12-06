# coding: utf-8
from logging import getLogger, NullHandler
logger = getLogger(__name__)
logger.addHandler(NullHandler())

import paho.mqtt.client
import json
import time
import os

class MqttController(object):

    def __init__(self, name, host, port=1883, user=None, password=None, ca_certs=None, certfile=None, keyfile=None, connect=True, rwt_use=True, rwt_retain=True, logger=logger):
        self.name = name
        self.port = int(port)
        self.host = host
        self.rwt_retain = rwt_retain
        self.logger = logger
        self.rwt_use = rwt_use

        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile = keyfile

        self.client = paho.mqtt.client.Client(protocol=paho.mqtt.client.MQTTv311)

        if user:
            self.logger.debug('set user: %s, passowrd: *****', user)
            self.client.username_pw_set(user, password=password)

        if ca_certs:
            self.logger.info('set tls.')
            self.client.tls_set(ca_certs=self.ca_certs, certfile=self.certfile, keyfile=self.keyfile)

        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        if self.rwt_use:
            self.client.will_set('stat/{0}/LWT'.format(self.name), payload='Offline', qos=1, retain=self.rwt_retain)

        if connect:
            self.connect()

    def connect(self):
        self.logger.debug('connect MQTT at %s:%s', self.host, self.port)
        for i in range(0, 255):
            try:
                self.client.connect(self.host, self.port)
            except Exception:
                time.sleep(1)
            else:
                return True

        raise IOError()


    def _on_connect(self, client, userdata, flags, respons_code):
        client.subscribe('+/{0}/#'.format(self.name))
        if self.rwt_use:
            client.publish('stat/{0}/LWT'.format(self.name), 'Online', retain=self.rwt_retain)

        self.on_connect(client, userdata, flags, respons_code)

    def on_connect(self, client, userdata, flags, respons_code):
        pass

    def _on_message(self, client, userdata, msg):
        r = parse(msg.topic)
        if r is None:
            self.logger.debug('pass %s', msg.topic)
            return

        self._recieve_message(r, msg)

    def _recieve_message(self, r, msg):
        if isinstance(msg.payload, bytes):
            payload = msg.payload.decode('utf-8')

        if r['prefix'].lower() == 'cmnd':
            self.command(r['topic'], payload)
            return

    def disconnect(self):
        self.client.disconnect()

    def publish(self, *args, **kwargs):
        self.logger.debug(args)
        self.client.publish(*args, **kwargs)

    def publish_status(self, stat, message):
        topic = 'stat/{name}/{stat}'.format(
            name=self.name, stat=stat)
        self.publish(topic, message)

    def publish_result(self, stat, message='', payload={}):
        if not message and not payload:
            self.logger.debug('message and payload both Empty, I do nothing.')
            return

        if payload:
            message = json.dumps(payload)

        topic = 'result/{name}/{stat}'.format(
            name=self.name, stat=stat)
        self.publish(topic, message)

    def loop(self, block=True):
        self.logger.debug('loop start.')
        if block:
            self.client.loop_forever()
        else:
            self.client.loop_start()

    def disconnect(self):
        self.client.disconnect()

    def commandSend(self, name, command, payload=''):
        topic = 'cmnd/{0}/{1}'.format(name, command)
        self.client.publish(topic, payload)

    def command(self, command, payload):
        self.logger.debug('get command %s: %s', command, payload)

        try:
            try:
                func = getattr(self, 'cmd_{0}'.format(command.lower()))
                r = func(payload)
                if r is not None:
                    self.publish_status(command.lower(), r)

            except (TypeError, AttributeError):
                self._command(self.client, command, payload)

        except Exception as e:
            self.logger.exception(e)
            return 'error spawn'

    def cmd_status(self, payload):
        # self.publish_status('status', json.dumps(getStatus()))
        return None

    def cmd_ping(self, payload):
        return 'pong'

    def _command(self, client, command, payload):
        pass

import re

prefix = r'(?P<prefix>[a-zA-Z0-9._\-:;]+)/(?P<name>[a-zA-Z0-9._\-:;]+)/(?P<topic>[a-zA-Z0-9._:;]+)$'
topic_parser = re.compile(prefix)

def parse(topic):
    r = topic_parser.match(topic)
    if r is None:
        return None

    return {
        'prefix': r.group('prefix'),
        'name': r.group('name'),
        'topic': r.group('topic'),
    }

