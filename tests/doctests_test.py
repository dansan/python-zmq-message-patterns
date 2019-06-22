# -*- coding: utf-8 -*-

import unittest

testSuite = unittest.TestSuite()

import doctest
from zmessage import zmessage
from zmessage import pipeline
from tests import simple_pipeline_test

testSuite.addTest(doctest.DocTestSuite(zmessage))
testSuite.addTest(doctest.DocTestSuite(pipeline))
testSuite.addTest(doctest.DocTestSuite(simple_pipeline_test))
unittest.TextTestRunner(verbosity=2).run(testSuite)
