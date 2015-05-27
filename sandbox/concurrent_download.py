#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
from Queue import Queue
import shutil
from threading import Thread

import boto


def concatenate(infiles, outfile):
    with open(outfile, 'wb') as fout:
        for infile in infiles:
            with open(infile, 'rb') as fin:
                shutil.copyfileobj(fin, fout)


class Downloader(Thread):

    def __init__(self, queue):
        super(Downloader, self).__init__()
        self.queue = queue

    def run(self):
        s3 = boto.connect_s3()
        while True:
            try:
                outfile, key, start, end = self.queue.get()
                rhdrs = {'Range': 'bytes={}-{}'.format(start, end)}

                print rhdrs

                with open(outfile, 'wb') as f:
                    key.get_contents_to_file(f, headers=rhdrs)
                self.queue.task_done()
            except KeyboardInterrupt:
                break
            except Exception:
                break


class MultipartDownloader(object):

    def __init__(self, concurrency=4):
        self.s3 = boto.connect_s3()
        self.queue = Queue()
        self.downloaders = []
        for i in xrange(concurrency):
            t = Downloader(self.queue)
            t.daemon = True
            t.start()
            self.downloaders.append(t)

    def __del__(self):
        self.queue.join()
            
    def get_s3_contents_to_file(self, bucket, key, outfile, part_size=20):
        k = self.s3.get_bucket(bucket).get_key(key)
        n_part = (int(k.size / part_size)
                  + (1 if k.size % part_size > 0 or k.size < part_size else 0))
        print k.size
        parts = []
        for i in xrange(n_part):
            fn = outfile + '.dpart' + str(i)
            self.queue.put((fn, k, i * part_size, min(k.size, (i + 1) * part_size) - 1))
            parts.append(fn)


def main():

    dldr = MultipartDownloader()
    dldr.get_s3_contents_to_file('skope-taro', 'testbandit/output/part-00000',
                                 '/home/taro/tmp/mdtest')



if __name__ == '__main__':
    main()
