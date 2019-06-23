python-zmq-message-patterns
===========================

Library to quickly build ZeroMQ based Python applications.

Introduction
------------

Library to make writing applications using `ZeroMQ <http://www.zeromq.org/>`_ message patterns through `PyZMQ <https://github.com/zeromq/pyzmq>`_ easier.

TODO: explain ``ZMessage`` and ``ZNode`` classes.

Install
-------

Install through pip::

  pip install zmq-message-patterns

Pipeline
--------

A ventilator sends jobs to multiple worker processes, which send the results to a sink.

Channels:

* ventilator -> worker: jobs for the workers
* ventilator -> sink: IDs of jobs sent to workers, so sink knows if all jobs have completed
* worker -> sink: results
* sink -> worker: sink sends shutdown command, when finished

Diagram::

       ventilator-------------+
           |                  |
   +-------+------+           |
   |       |      |           |
 worker worker worker  ...    |
   ||      ||    ||           |
   ++------++----++           |
           ||                 |
          sink----------------+


An is a fully functional example in the examples directory (``examples/pipeline_example.py``).
