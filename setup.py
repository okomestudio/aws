#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from setuptools import setup


VERSION = (0, 1, 0, "alpha")


def get_requirements():
    reqs = []
    with open('requirements.txt', mode='r') as f:
        for line in f:
            line = line.rstrip()
            if (line.startswith('#')
                or line.startswith('-e ')
                or not line):
                continue
            reqs.append(line)
    return reqs


setup(
    name='aws',
    description='AWS utilities',
    packages=[
        'aws',
    ],
    scripts=[
        'bin/s3_copy.py',
        'bin/s3_delete_all.py',
        'bin/s3_list.py',
    ],
    version='.'.join(filter(None, map(str, VERSION))),
    author='Taro Sato',
    author_email='taro@okomestudio.net',
    url='http://github.com/okomestudio/aws',
    install_requires=get_requirements(),
)
