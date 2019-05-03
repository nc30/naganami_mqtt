# coding: utf-8
from logging import getLogger
libLogger = getLogger(__name__)

import paho.mqtt.client
import json
import time
import os
import re

class MqttController(object):
    waches = []

    def __init__(self, name, host,
                port=1883, user=None, password=None,
                ca_certs=None, certfile=None, keyfile=None,
                connect=True, rwt_use=True, rwt_retain=True, logger=libLogger
        ):
        self.logger = libLogger
        self.name = name
        self.port = int(port)
        self.host = host
        self.rwt_retain = rwt_retain
        self.logger = logger
        self.rwt_use = rwt_use

        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile = keyfile

        self.client = paho.mqtt.client.Client(client_id=self.name, protocol=paho.mqtt.client.MQTTv311)

        if user:
            self.logger.debug('set user: %s, passowrd: *****', user)
            self.client.username_pw_set(user, password=password)

        if ca_certs:
            self.logger.info('set tls.')
            self.client.tls_set(ca_certs=self.ca_certs, certfile=self.certfile, keyfile=self.keyfile)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
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
        try:
            self._set_subscribe_topics(client, userdata, flags, respons_code)
            self.on_connect(client, userdata, flags, respons_code)
        except Exception as e:
            self.logger.exception(e)

    def set_wait(self, topics, callback, exipre=10):
        self.logger.debug('set_wait %s', topics)
        if type(topics) not in [tuple, list] or type(exipre) not in [int, float]:
            return False

        self.waches += [{
            "topics": topics,
            "callback": callback,
            "expire_in": time.time() + exipre
        }]
        self.logger.debug(self.waches)
        return True

    def _check_waits(self, client, userdata, msg):
        self.waches = list(filter(lambda w: w['expire_in'] > time.time(), self.waches))

        target = None
        for i in range(len(self.waches)):
            if msg.topic in self.waches[i]['topics']:
                target = self.waches.pop(i)
                break

        if target:
            target['callback'](client, userdata, msg)
            return True
        return False

    def _set_subscribe_topics(self, client, userdata, flags, respons_code):
        client.subscribe('+/{0}/#'.format(self.name))
        if self.rwt_use:
            client.publish('stat/{0}/LWT'.format(self.name), 'Online', retain=self.rwt_retain)

    def on_connect(self, client, userdata, flags, respons_code):
        pass

    def _on_disconnect(self, client, userdata, rc):
        self.logger.debug('disconnected rc=%s', rc)
        self.on_disconnect(self, client, userdata, rc)

    def on_disconnect(self, client, userdata, rc):
        pass

    def _on_message(self, client, userdata, msg):
        self.logger.debug('receive message. %s, %s', msg.topic, msg.payload)
        try:
            if self._check_waits(client, userdata, msg):
                return

            r = self._parse_topics(client, userdata, msg)
            if r:
                return

            r = self.on_message(client, userdata, msg)
            if r:
                return

            self.logger.debug('pass %s.', msg.topic)
        except Exception as e:
            self.logger.exception(e)

    def _parse_topics(self, client, userdata, msg):
        r = parse(msg.topic)
        if r is None:
            return False

        self._recieve_message(r, msg)
        return True

    def on_message(self, client, userdata, msg):
        return None

    def _recieve_message(self, r, msg):
        if isinstance(msg.payload, bytes):
            try:
                payload = msg.payload.decode('utf-8')
            except:
                payload = msg.payload

        if r['prefix'].lower() == 'cmnd':
            self._command(r['topic'], payload)

    def disconnect(self):
        self.client.disconnect()

    def publish(self, *args, **kwargs):
        self.logger.debug('publish %s', args)
        self.client.publish(*args, **kwargs)

    def publish_status(self, stat, message):
        topic = 'stat/{name}/{stat}'.format(
            name=self.name, stat=stat)
        self.publish(topic, message)

    def publish_result(self, stat, message='', payload={}, topic=None):
        if not message and not payload:
            self.logger.debug('message and payload both Empty, I do nothing.')
            return

        if payload:
            message = json.dumps(payload)

        if not topic:
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

    def _command(self, command, payload):
        self.logger.debug('get command %s: %s', command, payload)

        try:
            c = command.lower().replace('/', '_')
            func = getattr(self, 'cmd_{0}'.format(c))
            try:
                r = func(payload)
            except Exception as e:
                self.logger.exception(e)
                r = 'error'

        except (TypeError, AttributeError):
            try:
                r = self.command(self.client, command, payload)
            except Exception as e:
                self.logger.exception(e)
                r = 'error'

        if r is not None:
            if type(r) == tuple:
                payload == r[0]
                topic = r[1]
            else:
                payload = r
                topic = None

            self.publish_result(command.lower(), payload, topic=topic)


    def cmd_naganami_sama(self, payload):
        return 'kawaii'

    def cmd_ping(self, payload):
        return 'pong', 'result/{0}/pong'.format(self.name)

    def command(self, client, command, payload):
        return None


prefix = r'(?P<prefix>[^\/]+)/(?P<name>[^\/]+)/(?P<topic>[^\+#]+)$'
topic_parser = re.compile(prefix)

def parse(topic):
    r = topic_parser.match(topic)
    if r is None:
        return None

    return {
        'prefix': r.group('prefix'),
        'name': r.group('name'),
        'topic': r.group('topic')
    }
