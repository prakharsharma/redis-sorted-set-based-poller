"""
Base class that provides interface for a worker that uses a Redis sorted
set as a queue, polls it, works on a item when found.
"""

import time


DEFAULT_SLEEP_DURATION_SECONDS = 1


class RedisSortedSetQueuePoller(object):

    SNAPSHOT_SUFFIX = '-snapshot'

    def __init__(self, redis_client, redis_key, reverse=False,
                 sleep_duration=DEFAULT_SLEEP_DURATION_SECONDS,
                 logger=None):
        self.logger = logger
        self._redis = redis_client
        self._sorted_set_key = redis_key
        self._reverse = reverse
        self._sleep_duration = float(sleep_duration)
        self._snapshot_key = '%s%s' % (self._sorted_set_key,
                                       self.SNAPSHOT_SUFFIX)
    def __repr__(self):
        return "%s(redis_client=%s, redis_key=%s, reverse=%s, " \
               "sleep_duration=%s)" % (self.__class__.__name__, self._redis,
                                       self._sorted_set_key, self._reverse,
                                       self._sleep_duration)

    def log(self, level, message):
        try:
            if not hasattr(self, 'logger'):
                return
            logger = self.logger
            if logger is None:
                return
            if not hasattr(logger, level):
                return
            logger_method = getattr(logger, level)
            if not logger_method:
                return
            logger_method(message)
        except:
            pass

    def dequeue(self):
        """
        returns the next item to work on. If reverse=True, uses zrevrange,
        otherwise uses zrange
        :return: a pair of (score, member) of the item to work on
        """
        method = 'zrevrange' if self._reverse else 'zrange'
        self._next_item = (None, None)

        def _worker(pipe):
            try:
                self._next_item = getattr(pipe, method)(
                    self._sorted_set_key, 0, 0, withscores=True
                )[-1]
                self._redis.zunionstore(self._snapshot_key,
                                        [self._sorted_set_key])
                self.log('info', 'created snapshot %s from %s' % (
                    self._snapshot_key, self._sorted_set_key))
                if self.ready_to_process(self._next_item[1], self._next_item[
                        0]):
                    pipe.zrem(self._sorted_set_key, self._next_item[0])
                else:
                    self._next_item = (None, None)
            except IndexError:
                # sorted set doesn't exist yet; it's OK, ignore it
                self.log('info', "Polling queue doesn't exist")
            except Exception:
                import traceback
                self.log('error', traceback.format_exc())

        self._redis.transaction(_worker, self._sorted_set_key,
                                self._snapshot_key)
        return self._next_item[1], self._next_item[0]

    def enqueue(self, score, member):
        def _worker(pipe):
            pipe.zadd(self._sorted_set_key, score, member)

        self._redis.transaction(_worker, self._sorted_set_key)

    def ready_to_process(self, score, member):
        raise NotImplementedError()

    def process(self, score, member):
        raise NotImplementedError()

    def run(self):
        if self._redis.exists(self._snapshot_key) and \
                (self._redis.zcard(self._sorted_set_key) !=
                    self._redis.zcard(self._snapshot_key)):
            self.log('info', 'some members were lost between runs. Replacing '
                             '%s with %s' % (self._sorted_set_key,
                                             self._snapshot_key))
            self._redis.zunionstore(self._sorted_set_key, [self._snapshot_key])
        self.log('info', 'starting polling loop for key: %s in Redis: %s' % (
            self._sorted_set_key, self._redis))
        try:
            self._run()
        except KeyboardInterrupt:
            self.log('info', 'received KeyboardInterrupt')
            if self._next_item[0] is not None:
                self.enqueue(self._next_item[1], self._next_item[0])
                self.log('info', 'enqueued (%s, %s) while handling '
                                 'KeyboardInterrupt' % (self._next_item[1],
                                                        self._next_item[0]))
        self.log('info', 'ending polling loop for key: %s in Redis: %s' % (
            self._sorted_set_key, self._redis))

    def _run(self):
        sleep_between_runs = False
        self._next_item = (None, None)
        while True:
            if sleep_between_runs:
                self.log('info', 'sleeping for %s seconds' %
                         self._sleep_duration)
                time.sleep(self._sleep_duration)
            try:
                score, member = self.dequeue()
                if score is not None:
                    self.process(score, member)
                    sleep_between_runs = False
                    self.log('info', 'removing item from snapshot')
                    self._redis.zrem(self._snapshot_key, member)
                else:
                    sleep_between_runs = True
                self._next_item = (None, None)
            except Exception as exn:
                self.log('error', 'Error encountered when processing item: '
                                  '%s' % exn)
                import traceback
                self.log('error', traceback.format_exc())
                sleep_between_runs = True
                self.log('warning', 'Polling dequeue() or process() have not '
                                    'been executed')

