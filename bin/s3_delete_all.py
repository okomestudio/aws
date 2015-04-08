#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""Delete multiple keys.

Examples
--------

  $ s3_delete_keys.py somebucket a b

will delete all keys prefixed by 'a' and 'b' in bucket somebucket.

"""
import gevent.monkey
gevent.monkey.patch_all()

from argparse import ArgumentParser
import logging

import gevent
import gevent.pool

from aws.s3 import s3


log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
log.addHandler(ch)


def main(args):
    pool_size = 10
    group = gevent.pool.Pool(len(args.key_prefix))

    def task_group(group_id, bucket_name, prefix):
        pool = gevent.pool.Pool(10)

        def delete_keys(bucket_name, key_names):
            if len(key_names) == 0:
                return
            log.info(u'deleting keys from {} to {} in {}'
                     .format(key_names[0], key_names[-1], bucket_name))
            retry = 3
            for i in xrange(retry):
                try:
                    s3.delete_keys(bucket_name, key_names)
                except Exception, e:
                    log.warn(u'error: {}, retry {}'.format(repr(e), i + 1))
                    continue
                else:
                    break
            else:
                raise

        def log_qsize(wait):
            while True:
                log.info(u'{}: number of concurrent delete tasks = {}'
                         .format(group_id, pool_size - pool.free_count()))
                gevent.sleep(wait)

        g_log_qsize = gevent.spawn(log_qsize, 15)

        key_names = []
        for i, key in enumerate(s3.list(bucket_name, prefix=prefix, marker=args.marker)):
            key_names.append(key.name)
            if len(key_names) < 1000:
                continue
            pool.spawn(delete_keys, bucket_name, key_names)
            key_names = []
        pool.spawn(delete_keys, bucket_name, key_names)
        pool.join()
        g_log_qsize.kill()
        g_log_qsize.join()

    bucket_name = args.bucket
    for group_id, prefix in enumerate(args.key_prefix):
        group.spawn(task_group, group_id, bucket_name, prefix)
    group.join()


if __name__ == '__main__':
    p = ArgumentParser(description=__doc__.strip())
    p.add_argument('bucket', type=str,
                   help='S3 bucket name')
    p.add_argument('key_prefix', type=str, nargs='*', default=[''],
                   help='S3 key prefix(es)')
    p.add_argument('--marker', type=str, default='',
                   help='resume iteration from given key') 
    main(p.parse_args())
