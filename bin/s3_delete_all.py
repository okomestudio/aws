#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""Delete multiple keys.

"""
import gevent.monkey
gevent.monkey.patch_all()

from argparse import ArgumentParser

import boto
import gevent

from aws.s3 import s3


def main(args):
    bucket = boto.connect_s3().get_bucket(args.bucket)
    for prefix in args.key_prefix:
        for i, key in enumerate(bucket.list(prefix=prefix, marker=args.marker)):
            if i > 1000:
                break
            print key.name


if __name__ == '__main__':
    p = ArgumentParser(description=__doc__.strip())
    p.add_argument('bucket', type=str,
                   help='S3 bucket name')
    p.add_argument('key_prefix', type=str, nargs='*', default=[''],
                   help='S3 key prefix(es)')
    p.add_argument('--marker', type=str, default='',
                   help='resume iteration from given key') 
    main(p.parse_args())
