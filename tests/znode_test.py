# -*- coding: utf-8 -*-

import os
import random
import string
import unittest

import zmq

from zmessage.znode import ZNode


def random_string(length=8):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))


class ZTest(ZNode):
    def run(self, *args, **kwargs):
        pass


class TestZNode(unittest.TestCase):
    def test_add_socket(self):
        args = (
            random_string(),
            random.choice(('bind', 'connect')),
            random.choice(('DEALER', 'PAIR', 'PUB', 'PULL', 'PUSH', 'REP', 'REQ', 'ROUTER', 'SUB')),
            '{}://{}'.format(random.choice(('inproc', 'ipc', 'tcp', 'pgm', 'epgm')), random_string())
        )
        kwargs = {
            random_string(): random_string(),
            random_string(): random_string()
        }
        zt = ZTest()
        zt.add_socket(*args, **kwargs)
        sc = zt._socket_configs[-1]
        self.assertEqual(args[0], sc.name)
        self.assertEqual(args[1], sc.method)
        self.assertEqual(getattr(zmq, args[2]), sc.type)
        self.assertEqual(args[3], sc.addr)
        self.assertEqual(kwargs, sc.attrs)

    def test_init(self):
        zt = ZTest()
        zt.init()
        self.assertEqual(zt.pid, os.getpid())
        self.assertIsNotNone(zt.context)
        self.assertFalse(zt.context.closed)

    def test_start(self):
        zt = ZTest()
        zt.start()
        self.assertIsNotNone(zt.context)
        self.assertTrue(zt.context.closed)

# TODO: test connect with random unix socket


if __name__ == '__main__':
    unittest.main(verbosity=2)
