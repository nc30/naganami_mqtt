
from setuptools import setup

requires = ['paho-mqtt']

setup(
    name = "naganami_mqtt",
    version = '0.0.3',
    install_requires = requires,
    author = 'Himura Asahi',
    author_email = 'himura@nitolab.com',
    packages = ["naganami_mqtt"],
    description = "naganami sama type mqtt controller",
    url = "https://github.com/nc30/naganami_mqtt"
)
