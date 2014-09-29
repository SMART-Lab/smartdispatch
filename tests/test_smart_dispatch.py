import os
import unittest
import tempfile
import shutil

from subprocess import call

from nose.tools import assert_true, assert_equal


class TestSmartdispatcher(unittest.TestCase):

    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_main(self):
        cd_command = "cd " + self.testing_dir
        command = 'smart_dispatch.py --pool 10 -q qtest@mp2 -n 5 -x echo "1 2 3 4" "6 7 8" "9 0"'
        assert_equal(call(cd_command + " ; " + command, shell=True), 0)

        assert_true(os.path.isdir(os.path.join(self.testing_dir, '')))
