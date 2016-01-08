# Overview

Provides a utility class that uses a Redis sorted set as a queue, polls it
and processes items that are ready to be processed. The poller is resilient
across Redis restarts. That is you don't lose an item if it was dequeued from
the sorted set but Redis died/crashed/got killed before item was fully
processed. To write your own poller

- derive from RedisSortedSetQueuePoller
- implement `ready_to_process`
- implement `process`

