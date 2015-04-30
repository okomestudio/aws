#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""List keys under a bucket concurrently.

Examples
--------

  $ s3_list.py s3://somebucket/

"""
import gevent.monkey
gevent.monkey.patch_all()

from argparse import ArgumentParser
import logging
import string

import gevent
import gevent.pool

from aws.s3 import s3, S3URI


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
log.addHandler(ch)


def main(args):
    src = S3URI(args.uri)

    prefixes = [c for c in args.valid_chars]
    for i in xrange(args.num_prefix_chars - 1):
        ps = []
        for prefix in prefixes:
            for c in args.valid_chars:
                ps.append(c + prefix)
        prefixes = ps
    
    def task_group(group_id, bucket_name, prefix):
        for i, key in enumerate(s3.list(bucket_name, prefix=prefix)):
            print key.name

    group = gevent.pool.Pool(len(prefixes))
    for group_id, prefix in enumerate(prefixes):
        prefix = ''.join([src.key_name, prefix])
        group.spawn(task_group, group_id, src.bucket_name, prefix)
    group.join()


if __name__ == '__main__':
    p = ArgumentParser(description=__doc__.strip())
    p.add_argument(
        'uri', type=str,
        help='S3 URI'
    )
    p.add_argument(
        '--valid-chars', type=unicode,
        default=unicode(string.ascii_letters + string.digits),
        help='Valid unicode characters used for key names.'
    )
    p.add_argument(
        '--num-prefix-chars', type=int, default=2,
        help='Number of prefix characters used for concurrent requests'
    )
    main(p.parse_args())
