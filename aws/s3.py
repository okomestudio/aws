#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
import contextlib
import logging
import os
import re
import sys
import time

import boto
import gevent
import gevent.queue


log = logging.getLogger(__name__)


def ensure_unicode(s, encoding='utf-8'):
    return s if isinstance(s, unicode) else s.decode(encoding)


class S3URI(unicode):

    validate = re.compile(ur'^s3://(.+)$', re.U)

    def __init__(self, uri):
        b, k = self.split_s3_path(uri)
        self.bucket_name = b
        self.key_name = k

    @classmethod
    def split_s3_path(cls, uri):
        m = cls.validate.search(uri)
        if not m:
            raise ValueError(u'Invalid S3 URI ({})'.format(uri))
        ts = m.group(1).split(u'/', 1)
        bucket = ts[0]
        key = ts[1] if len(ts) > 1 else u''
        return ensure_unicode(bucket), ensure_unicode(key)

    def __unicode__(self):
        return os.path.join(u's3://', self.bucket_name, self.key_name)

    def __str__(self):
        return self.__unicode__.encode('utf-8')


class S3Manager(object):

    def __init__(self):
        self._connq = gevent.queue.Queue()
        self._t_scaledown = 60.0
        gevent.spawn(self._log_qsize)

    def _log_qsize(self):
        log.debug('s3 connection queue size: {}'.format(self._connq.qsize()))
        gevent.sleep(60)

    @contextlib.contextmanager
    def client(self):
        conn = self._get_conn()
        try:
            yield conn
        finally:
            self._put_conn(conn)

    def _get_conn(self):
        try:
            while True:
                conn, last_used = self._connq.get_nowait()
                dt = time.time() - last_used
                if dt < self._t_scaledown:
                    break
                else:
                    log.debug('discarding an s3 connection (inactive for {} sec)'
                              .format(dt))
                    conn.close()
        except gevent.queue.Empty:
            conn = _S3Connection()
            log.warning('adding a new s3 connection')
        return conn

    def _put_conn(self, conn):
        if conn.failed:
            pass
        else:
            log.debug('putting back an s3 connection to queue ({})'
                      .format(self._connq.qsize()))
            self._connq.put((conn, time.time()))

    def _operate(self, method, *args, **kwargs):
        with self.client() as c:
            err3 = None
            try:
                gevent.sleep(0)
                return getattr(c, method)(*args, **kwargs)
            except Exception:
                err3 = sys.exc_info()
                log.warn(' '.join(map(repr, err3)))
        raise err3[0], err3[1], err3[2]

    def __getattr__(self, name):
        if name in _S3Connection.OPERATIONS:
            def wrapper(*args, **kwargs):
                return self._operate(name, *args, **kwargs)
            return wrapper
        raise AttributeError('operation {} not allowed'.format(name))


def _set_failed_on_error(f):
    def _f(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except:
            self.failed = True
            raise
    return _f


class _S3Connection(object):

    OPERATIONS = ['get', 'list', 'put', 'copy', 'delete_keys']
                    
    def __init__(self):
        self._conn = boto.connect_s3()
        self._bucket = {}
        self.failed = False

    def close(self):
        self._conn.close()

    def get_bucket(self, bucket_name):
        if bucket_name in self._bucket:
            return self._bucket[bucket_name]
        bucket = self._conn.get_bucket(bucket_name)
        self._bucket[bucket_name] = bucket
        return bucket

    @_set_failed_on_error
    def get(self, bucket_name, key_name):
        key = self.get_bucket(bucket_name).get_key(key_name)
        return key.get_contents_as_string()

    @_set_failed_on_error
    def list(self, bucket_name, prefix='', delimiter='', marker='',
             headers=None, encoding_type=None):
        return self.get_bucket(bucket_name).list(prefix=prefix,
                                                 delimiter=delimiter,
                                                 marker=marker)

    @_set_failed_on_error
    def put(self, bucket_name, key_name, content):
        key = self.get_bucket(bucket_name).new_key(key_name)
        return key.set_contents_from_string(content)

    @_set_failed_on_error
    def copy(self, src_bucket_name, src_key_name,
             dst_bucket_name, dst_key_name):
        return (self.get_bucket(src_bucket_name).get_key(src_key_name)
                .copy(dst_bucket_name, dst_key_name))

    @_set_failed_on_error
    def delete_keys(self, bucket_name, key_names):
        return self.get_bucket(bucket_name).delete_keys(key_names)


s3 = S3Manager()
