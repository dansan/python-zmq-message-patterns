# -*- coding: utf-8 -*-

"""
Consume CPU time on pultiple cores:
Send a lists (batch) of ints to workers to multiply a lot of times.

This is the simple_pipeline_test with CPU time burned instead of slept.
All your CPUs/cores should be maxed out. Tweak NUM_JOBS and BATCH_SIZE
plus MULTIPLY_REPITITIONS to use more jobs or longer jobs.

      ventilator-------------+
          |                  |
  +-------+------+           |
  |       |      |           |
worker worker worker  ...    |
  |       |      |           |
  +-------+------+           |
          |                  |
        sink-----------------+

"""

from __future__ import absolute_import
import os
import time
import random
import logging
import shutil
import signal
import tempfile
import threading
from functools import partial
from collections import OrderedDict
from multiprocessing import cpu_count, Process
from zmessage.pipeline import Sink, Ventilator, VentilatorToWorkerMessage, Worker, WorkerToSinkMessage


NUM_WORKERS = max(2, cpu_count())
NUM_JOBS = 10000
MULTIPLY_REPITITIONS = 100000
BATCH_SIZE = 10


class MultiplyingVentilatorToWorkerMessage(VentilatorToWorkerMessage):
    types = ('multiply',)
    required_data = ('multiplicands',)


class MultiplyingWorkerToSinkMessage(WorkerToSinkMessage):
    types = ('job done',)
    required_data = ('ventilator_request_id', 'results')


class MultiplyingVentilator(Ventilator):
    def __init__(self, jobs_to_workers_addr, job_ids_to_sink_addr):  # type: (str, str) -> None
        super(MultiplyingVentilator, self).__init__(jobs_to_workers_addr, job_ids_to_sink_addr)
        self.jobs_sent = 0

    def requests(self):  # type: () -> Iterator[VentilatorWorkerMessageType]
        while self.jobs_sent < NUM_JOBS:
            yield MultiplyingVentilatorToWorkerMessage(
                'multiply',
                multiplicands=[random.randint(2, 10) for _ in range(BATCH_SIZE)]
            )
            self.jobs_sent += BATCH_SIZE


class MultiplyingWorker(Worker):
    VentilatorWorkerMessageCls = MultiplyingVentilatorToWorkerMessage

    def __init__(self, *args, **kwargs):
        super(MultiplyingWorker, self).__init__(*args, **kwargs)
        self.requests_count = 0
        self.results_count = 0

    def do_work(self, request):  # type: (VentilatorWorkerMessageType) -> WorkerSinkMessageType
        self.requests_count += 1
        if self.requests_count % 100 == 0:
            self.logger.debug('[%s] Received requests: %r', self.name, self.requests_count)
        results = [0 for _ in range(len(request['multiplicands']))]
        for index, num in enumerate(request['multiplicands']):
            for _ in range(MULTIPLY_REPITITIONS):
                results[index] = num * num
        self.results_count += 1
        return MultiplyingWorkerToSinkMessage(
            mtype='job done',
            ventilator_request_id=request.id,
            results=results
        )


class MultiplyingSink(Sink):
    _printed = 0.0
    _log_interval = 10.0

    def handle_result(self, request):  # type: (WorkerSinkMessageType) -> None
        # self.logger.debug('[Sink] Received results: %r', request['results'])
        self.print_open_requests()

    def print_open_requests(self):
        now = time.time()
        if self._printed + self._log_interval < now:
            self._printed = now
            self.logger.info(
                '[%s] Waiting for %d unfinished requests (and %d unknown)...',
                self.name,
                len(self.unfinished_request_ids),
                len(self.unknown_ventilator_request_ids)
            )

    def run(self, *args, **kwargs):  # type: (*Any, **Any) -> Any
        self._printed = time.time()
        super(MultiplyingSink, self).run(*args, **kwargs)


def kill_processes(signum, frame):  # type: (int, Any) -> None
    for p in workers.values():
        try:
            os.kill(p.pid, signal.SIGINT)
        except OSError as exc:
            import errno
            # ignore "no such process" as it might have been joined already
            if exc.errno != errno.ESRCH:
                raise


if __name__ == "__main__":
    logger = logging.getLogger('zmessage')
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(sh)

    socket_dir = tempfile.mkdtemp()
    jobs_for_workers_addr = 'ipc://{}'.format(os.path.join(socket_dir, 'jobs_for_workers.socket'))
    job_ids_for_sink_addr = 'ipc://{}'.format(os.path.join(socket_dir, 'job_ids_for_sink.socket'))
    workers_results_addr = 'ipc://{}'.format(os.path.join(socket_dir, 'workers_results.socket'))
    workers_control_addr = 'ipc://{}'.format(os.path.join(socket_dir, 'workers_control.socket'))

    # start workers in separate processes
    workers = OrderedDict()
    for num in range(NUM_WORKERS):
        worker = MultiplyingWorker(jobs_for_workers_addr, workers_control_addr, workers_results_addr)
        worker.name = 'Worker {}'.format(num + 1)
        p = Process(target=worker.start, name=worker.name)
        p.start()
        workers[worker] = p
        logger.info('Started worker %d/%d (PID %d, name %r).', num + 1, NUM_WORKERS, p.pid, worker.name)

    t0 = time.time()

    # start ventilator in thread of main process
    ventilator = MultiplyingVentilator(jobs_for_workers_addr, job_ids_for_sink_addr)
    ventilator_thread = threading.Thread(target=partial(ventilator.start, cleanup=False))
    ventilator_thread.start()
    logger.info('Started ventilator (in background thread of main process).')

    # start sink (blocking in main process)
    sink = MultiplyingSink(workers_results_addr, workers_control_addr, job_ids_for_sink_addr)
    logger.info('Starting sink (in main process) and waiting for it to finish.')
    sink.start()
    logger.info('Sink has shutdown.')

    t1 = time.time()

    # shutdown
    ventilator.cleanup()
    ventilator_thread.join()
    logger.info('Ventilator has shutdown.')

    # stop workers, if they didn't already
    previous_handler = signal.signal(signal.SIGALRM, kill_processes)

    logger.info('Telling for workers to shutdown (sending SIGALRM) if they haven\'t already...')
    for count, process in enumerate(workers.values()):
        signal.alarm(1)
        try:
            process.join()
        except KeyboardInterrupt:
            logger.warn('KeyboardInterrupt: Sending SIGINT to all workers...')
            for p in workers.values():
                os.kill(p.pid, signal.SIGINT)
            process.join()
        logger.info('Worker %d/%d with PID %d and name %r ended.', count + 1, len(workers), process.pid, process.name)
    logger.info('All workers have shutdown.')

    signal.signal(signal.SIGALRM, previous_handler)

    logger.info('Wall time elapsed: %0.2f', t1 - t0)

    shutil.rmtree(socket_dir)
