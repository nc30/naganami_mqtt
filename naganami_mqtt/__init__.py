from logging import getLogger, NullHandler
libLogger = getLogger('naganami_mqtt')
libLogger.addHandler(NullHandler())

from .mqtt_base import MqttController
from . import awsiot

__version__ = '0.0.6'
