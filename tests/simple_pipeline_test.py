# -*- coding: utf-8 -*-

#
#       ventilator-------------+
#           |                  |
#   +-------+------+           |
#   |       |      |           |
# worker worker worker  ...    |
#   |       |      |           |
#   +-------+------+           |
#           |                  |
#         sink-----------------+
#

import os
import time
import random
import signal
import logging
import shutil
import tempfile
import threading
import unittest
from collections import OrderedDict
from multiprocessing import cpu_count, Process

from zmessage.pipeline import Sink, Ventilator, VentilatorToWorkerMessage, Worker, WorkerToSinkMessage
from .testlogging import setup_logging, update_formatter

try:
    from typing import Any, Iterator, Optional, Tuple, Type
    from zmessage.pipeline import VentilatorWorkerMessageType, WorkerSinkMessageType
except ImportError:
    pass


workers = OrderedDict()
NUM_WORKERS = max(2, min(4, cpu_count() - 1))
NUM_JOBS = 10000


class TestVentilatorToWorkerMessage(VentilatorToWorkerMessage):
    types = ('sleep',)
    required_data = ('sleep',)

    def is_valid_sleep(self):  # type: () -> Tuple[bool, str]
        """
        >>> TestVentilatorToWorkerMessage('foo', foo='bar').is_valid()
        (False, "Value of 'type' must be one of sleep.")
        >>> TestVentilatorToWorkerMessage('sleep', foo='bar').is_valid()
        (False, "Required data 'sleep' is unset in message.")
        >>> TestVentilatorToWorkerMessage('sleep', sleep='bar').is_valid()
        (False, "'sleep' must be a float.")
        >>> TestVentilatorToWorkerMessage('sleep', sleep=2).is_valid()
        (True, '')
        """
        try:
            float(self.get('sleep'))   # type: ignore  # noqa
        except (TypeError, ValueError):
            return False, "'sleep' must be a float."
        return True, ''


class TestWorkerToSinkMessage(WorkerToSinkMessage):
    types = ('job done',)
    required_data = ('ventilator_request_id', 'slept')


class TestVentilator(Ventilator):
    def __init__(self, jobs_to_workers_addr, job_ids_to_sink_addr):  # type: (str, str) -> None
        super(TestVentilator, self).__init__(jobs_to_workers_addr, job_ids_to_sink_addr)
        self.jobs_sent = 0
        self.time_waited = 0.0

    def requests(self):  # type: () -> Iterator[VentilatorWorkerMessageType]
        while self.jobs_sent < NUM_JOBS:
            yield TestVentilatorToWorkerMessage('sleep', sleep=random.random() / 500.0)
            self.jobs_sent += 1


class TestWorker(Worker):
    VentilatorWorkerMessageCls = TestVentilatorToWorkerMessage

    def __init__(self, jobs_in_addr, worker_control_addr, results_out_addr):  # type: (str, str, str) -> None
        super(TestWorker, self).__init__(jobs_in_addr, worker_control_addr, results_out_addr)
        self.time_waited = 0.0

    def do_work(self, request):  # type: (VentilatorWorkerMessageType) -> WorkerSinkMessageType
        time.sleep(request['sleep'])
        self.time_waited += request['sleep']
        return TestWorkerToSinkMessage(
            mtype='job done',
            ventilator_request_id=request.id,
            slept={self.name: self.time_waited}
        )

    def start(self, install_sig_handler=True, *args, **kwargs):  # type: (Optional[bool], *Any, **Any) -> Any
        # update PID in logger
        pid = os.getpid()
        if self.pid != pid:
            self.pid = pid
            update_formatter(logging.getLogger('zmessage'), pid)
        super(TestWorker, self).start(install_sig_handler, *args, **kwargs)


class TestSink(Sink):
    _printed = 0.0
    _log_interval = 10.0
    workers_slept = {}

    def handle_result(self, request):  # type: (WorkerSinkMessageType) -> None
        for k, v in request['slept'].items():
            if v > self.workers_slept.get(k, 0.0):
                self.workers_slept[k] = v
        self.print_open_requests()

    def print_open_requests(self):
        now = time.time()
        if self._printed + self._log_interval < now:
            self._printed = now
            self.logger.info(
                '[%s] Waiting for %d unfinished requests (and %d unknown)...',
                self.name,
                len(self.unfinished_request_ids), len(self.unknown_ventilator_request_ids)
            )

    def run(self, *args, **kwargs):  # type: (*Any, **Any) -> Any
        self._printed = time.time()
        super(TestSink, self).run(*args, **kwargs)


def kill_processes(signum, frame):  # type: (int, Any) -> None
    for p in workers.values():
        try:
            os.kill(p.pid, signal.SIGINT)
        except OSError as exc:
            import errno
            # ignore "no such process" as it might have been joined already
            if exc.errno != errno.ESRCH:
                raise


class TestSimplePipeline(unittest.TestCase):
    def setUp(self):
        # self.skipTest('disabled')

        self.socket_dir = tempfile.mkdtemp()
        self.jobs_for_workers_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'jobs_for_workers.socket'))
        self.job_ids_for_sink_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'job_ids_for_sink1_addr.socket'))
        self.workers_results_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'workers_results_addr.socket'))
        self.workers_control_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'workers_control.socket'))

    def tearDown(self):
        shutil.rmtree(self.socket_dir)

    def runTest(self):
        logger = setup_logging('zmessage', logging.DEBUG, os.getpid())

        # start workers in separate processes
        for num in range(NUM_WORKERS):
            worker = TestWorker(self.jobs_for_workers_addr, self.workers_control_addr, self.workers_results_addr)
            worker.name = 'Worker {}'.format(num + 1)
            p = Process(target=worker.start, name=worker.name)
            p.start()
            workers[worker] = p
            logger.info('Started worker %d/%d (PID %d, name %r).', num + 1, NUM_WORKERS, p.pid, worker.name)

        t0 = time.time()

        # start ventilator in thread of main process
        ventilator = TestVentilator(self.jobs_for_workers_addr, self.job_ids_for_sink_addr)
        ventilator_thread = threading.Thread(target=ventilator.start)
        ventilator_thread.start()
        logger.info('Started ventilator (in thread).')

        # start sink (blocking in main process)
        sink = TestSink(self.workers_results_addr, self.workers_control_addr, self.job_ids_for_sink_addr)
        sink.start()
        logger.info('Sink finished.')

        t1 = time.time()

        # shutdown
        ventilator_thread.join()

        # stop workers, if they didn't already
        previous_handler = signal.signal(signal.SIGALRM, kill_processes)

        for count, process in enumerate(workers.values()):
            signal.alarm(1)
            try:
                process.join()
            except KeyboardInterrupt:
                logger.warn('KeyboardInterrupt: Sending SIGINT to all workers...')
                for p in workers.values():
                    os.kill(p.pid, signal.SIGINT)
                process.join()
            logger.info('Worker %d/%d with PID %d and name %r ended.',
                        count + 1, len(workers), process.pid, process.name)

        signal.signal(signal.SIGALRM, previous_handler)

        logger.info('The End.')
        logger.info('')
        for name in sorted(sink.workers_slept.keys()):
            logger.info('%s slept %0.2f sec.', name, sink.workers_slept[name])
        logger.info('-' * 26)
        logger.info('%d workers slept  : %0.2f', len(sink.workers_slept), sum(sink.workers_slept.values()))
        logger.info('Wall time elapsed: %0.2f', t1 - t0)

        self.assertEqual(len(sink.unfinished_request_ids), 0, 'Not all requests reached the sink.')
        self.assertEqual(len(sink.unknown_ventilator_request_ids), 0, 'The sink received unknown requests.')
        self.assertLess(t1 - t0, sum(sink.workers_slept.values()), "Workers didn't sleep in parallel.")


if __name__ == '__main__':
    import doctest
    doctest.testmod()
    unittest.main(verbosity=2)
