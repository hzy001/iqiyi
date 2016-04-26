#!/usr/bin/env python
from distutils.core import setup
setup(
    name='mesos-junkman',
    version='0.1.0-centos7',
    author='The Mesos Team',
    author_email='<mesos@dev.qiyi.com>',
    install_requires=[
        'urllib3==1.15.1',
        'APScheduler==3.1.0'
    ],
    scripts=[
        './mesos_junkman.py'
    ],
    data_files=[
        ('/usr/etc',['conf/logging.conf']),
        ('/etc/systemd/system',['systemd/mesos-junkman.service'])
    ])
