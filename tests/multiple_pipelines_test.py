# -*- coding: utf-8 -*-

#
#        ventilatorA-------------+
#            |                   |
#   +--------+-------+           |
#   |        |       |           |
# workerA workerA workerA  ...   |
#   |        |       |           |
#   +--------+-------+           |
#            |                   |
#          sinkA-----------------+
#            &
#        ventilatorB-------------+
#            |                   |
#   +--------+-------+           |
#   |        |       |           |
# workerB workerB workerB  ...   |
#   |        |       |           |
#   +--------+-------+           |
#            |                   |
#          sinkB-----------------+
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
    from typing import Any, Iterator, Optional, Tuple, Type, TypeVar
    VentilatorWorkerMessageType = TypeVar('VentilatorWorkerMessageType', bound='VentilatorToWorkerMessage')
    WorkerSinkMessageType = TypeVar('WorkerSinkMessageType', bound='WorkerToSinkMessage')
except ImportError:
    pass


workersA = OrderedDict()
workersB = OrderedDict()
savb_process = None
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
    def __init__(self, jobs_in_addr, job_id_sink_addr):  # type: (str, str) -> None
        super(TestVentilator, self).__init__(jobs_in_addr, job_id_sink_addr)
        self.jobs_sent = 0

    def requests(self):  # type: () -> Iterator[VentilatorWorkerMessageType]
        while self.jobs_sent < NUM_JOBS:
            yield TestVentilatorToWorkerMessage('sleep', sleep=random.random() / 500.0)
            self.jobs_sent += 1


class TestWorker(Worker):
    VentilatorWorkerMessageCls = TestWorkerToSinkMessage

    def __init__(self, jobs_in_addr, worker_control_addr, results_out_addr):  # type: (str, str, str) -> None
        super(TestWorker, self).__init__(jobs_in_addr, worker_control_addr, results_out_addr)
        self.time_waited = 0.0

    def do_work(self, request):  # type: (VentilatorWorkerMessageType) -> WorkerSinkMessageType
        time.sleep(request['sleep'])
        self.time_waited += request['sleep']
        slept = request.get('slept') or {}
        slept[self.name] = self.time_waited
        return TestWorkerToSinkMessage(mtype='job done', ventilator_request_id=request.id, slept=slept)

    def start(self, install_sig_handler=True, *args, **kwargs):  # type: (Optional[bool], *Any, **Any) -> Any
        # update PID in logger
        pid = os.getpid()
        if self.pid != pid:
            self.pid = pid
            update_formatter(logging.getLogger('zmessage'), os.getpid())
        super(TestWorker, self).start(install_sig_handler, *args, **kwargs)


class TestSinkB(Sink):
    _printed = 0.0
    _log_interval = 10.0

    def __init__(self, *args, **kwargs):
        super(TestSinkB, self).__init__(*args, **kwargs)
        self.workers_slept = {}

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
        super(TestSinkB, self).run(*args, **kwargs)


class TestSinkAVentilatorB(TestSinkB):
    def __init__(self, ventilator, *args, **kwargs):  # type: (TestVentilator, *Any, **Any) -> None
        super(TestSinkAVentilatorB, self).__init__(*args, **kwargs)
        self.ventilator = ventilator

    def init(self, install_sig_handler=True):  # type: (Optional[bool]) -> None
        super(TestSinkAVentilatorB, self).init(install_sig_handler)
        self.ventilator.context = self.context
        self.ventilator.init(install_sig_handler)

    def cleanup(self):  # type: () -> None
        self.ventilator.cleanup()
        super(TestSinkAVentilatorB, self).cleanup()

    def handle_result(self, request):  # type: (WorkerSinkMessageType) -> None
        for k, v in request['slept'].items():
            if v > self.workers_slept.get(k, 0.0):
                self.workers_slept[k] = v
        self.print_open_requests()
        self.ventilator.send_job(
            TestVentilatorToWorkerMessage('sleep', sleep=random.random() / 500.0, slept=self.workers_slept)
        )

    def run(self, *args, **kwargs):  # type: (*Any, **Any) -> Any
        super(TestSinkAVentilatorB, self).run(*args, **kwargs)
        self.ventilator.logger.debug('[%s] Ventilator finished sending requests: %r.',
                                     self.ventilator.name, self.ventilator.request_count)
        self.ventilator.logger.debug('[%s] Telling sink B we have finished sending.', self.name)
        self.ventilator.send_finished()

    def start(self, install_sig_handler=True, *args, **kwargs):  # type: (Optional[bool], *Any, **Any) -> Any
        # update PID in logger
        pid = os.getpid()
        if self.pid != pid:
            self.pid = pid
            update_formatter(logging.getLogger('zmessage'), os.getpid())
        super(TestSinkAVentilatorB, self).start(install_sig_handler, *args, **kwargs)


def kill_processesA(signum, frame):  # type: (int, Any) -> None
    for p in workersA.values():
        try:
            os.kill(p.pid, signal.SIGINT)
        except OSError as exc:
            import errno
            # ignore "no such process" as it might have been joined already
            if exc.errno != errno.ESRCH:
                raise


def kill_processesB(signum, frame):  # type: (int, Any) -> None
    for p in workersB.values():
        try:
            os.kill(p.pid, signal.SIGINT)
        except OSError as exc:
            import errno
            # ignore "no such process" as it might have been joined already
            if exc.errno != errno.ESRCH:
                raise


def kill_savb(signum, frame):  # type: (int, Any) -> None
    try:
        os.kill(savb_process.pid, signal.SIGINT)
    except OSError as exc:
        import errno
        # ignore "no such process" as it might have been joined already
        if exc.errno != errno.ESRCH:
            raise


class TestMultiplePipeline(unittest.TestCase):
    def setUp(self):
        self.skipTest('disabled')

        self.socket_dir = tempfile.mkdtemp()
        self.jobs_for_workersA_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'jobs_for_workersA.socket'))
        self.job_ids_for_sinkA_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'job_ids_for_sinkA_addr.socket'))
        self.workersA_results_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'workersA_results_addr.socket'))
        self.workersA_control_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'workersA_control.socket'))

        self.jobs_for_workersB_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'jobs_for_workersB.socket'))
        self.job_ids_for_sinkB_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'job_ids_for_sinkB_addr.socket'))
        self.workersB_results_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'workersB_results_addr.socket'))
        self.workersB_control_addr = 'ipc://{}'.format(os.path.join(self.socket_dir, 'workersB_control.socket'))

    def tearDown(self):
        shutil.rmtree(self.socket_dir)

    def runTest(self):
        global savb_process

        logger = setup_logging('zmessage', logging.DEBUG, os.getpid())

        # start workers in separate processes
        for char, processes in (('A', workersA), ('B', workersB)):
            for num in range(NUM_WORKERS):
                worker = TestWorker(
                    getattr(self, 'jobs_for_workers{}_addr'.format(char)),
                    getattr(self, 'workers{}_control_addr'.format(char)),
                    getattr(self, 'workers{}_results_addr'.format(char))
                )
                worker.name = 'Worker {} {}'.format(char, num + 1)
                p = Process(target=worker.start, name=worker.name)
                p.start()
                processes[worker] = p
                logger.info('Started worker %s %d/%d (PID %d, name %r).',
                            char, num + 1, NUM_WORKERS, p.pid, worker.name)

        t0 = time.time()

        # start ventilator A in thread of main process
        ventilator_a = TestVentilator(self.jobs_for_workersA_addr, self.job_ids_for_sinkA_addr)
        ventilator_a.name = 'Ventilator A'
        ventilator_thread = threading.Thread(target=ventilator_a.start)
        ventilator_thread.start()
        logger.info('Started ventilator A (in thread).')

        # start ventilator B + sink A in separate process
        ventilator_b = TestVentilator(self.jobs_for_workersB_addr, self.job_ids_for_sinkB_addr)
        ventilator_b.name = 'Ventilator B'
        sink_a_vent_b = TestSinkAVentilatorB(
            ventilator_b,
            self.workersA_results_addr,
            self.workersA_control_addr,
            self.job_ids_for_sinkA_addr
        )
        sink_a_vent_b.name = 'Sink A'
        savb_process = Process(target=sink_a_vent_b.start, name=sink_a_vent_b.name)
        savb_process.start()
        logger.info('Started SinkA/VentB process (PID %d, name %r).', savb_process.pid, savb_process.name)

        # start sink B (blocking in main process)
        sink_b = TestSinkB(self.workersB_results_addr, self.workersB_control_addr, self.job_ids_for_sinkB_addr)
        sink_b.name = 'Sink B'
        sink_b.start()
        logger.info('Sink B finished.')

        t1 = time.time()

        # shutdown
        ventilator_thread.join()

        previous_handler = None
        try:
            # stop sinkA+ventB, if it didn't already
            previous_handler = signal.signal(signal.SIGALRM, kill_savb)
            signal.alarm(1)
            savb_process.join()
            logger.info('SinkA / VentB with PID %d and name %r ended.', savb_process.pid, savb_process.name)

            # stop workers, if they didn't already
            for char, process_list, kill_func in (
                    ('A', workersA.values(), kill_processesA),
                    ('B', workersB.values(), kill_processesB)
            ):
                signal.signal(signal.SIGALRM, kill_func)

                for count, process in enumerate(process_list):
                    signal.alarm(1)
                    process.join()
                    logger.info('Worker %d/%d with PID %d and name %r ended.',
                                count + 1, len(process_list), process.pid, process.name)
        except KeyboardInterrupt:
            logger.warn('KeyboardInterrupt: Sending SIGINT to all workers...')
            for p in workersA.values():
                os.kill(p.pid, signal.SIGINT)
                p.join()
            for p in workersB.values():
                os.kill(p.pid, signal.SIGINT)
                p.join()

        if previous_handler:
            signal.signal(signal.SIGALRM, previous_handler)

        logger.info('The End.')

        logger.info('')
        for name in sorted(sink_b.workers_slept.keys()):
            logger.info('%s slept %0.2f sec.', name, sink_b.workers_slept[name])
        logger.info('-' * 26)
        logger.info('%d workers slept  : %0.2f', len(workersA) + len(workersB), sum(sink_b.workers_slept.values()))
        logger.info('Wall time elapsed: %0.2f', t1 - t0)

        self.assertEqual(len(sink_b.unfinished_request_ids), 0, 'Not all requests reached the sink.')
        self.assertEqual(len(sink_b.unknown_ventilator_request_ids), 0, 'The sink received unknown requests.')
        self.assertLess(t1 - t0, sum(sink_b.workers_slept.values()), "Workers didn't sleep in parallel.")


if __name__ == '__main__':
    import doctest
    doctest.testmod()
    unittest.main(verbosity=2)
