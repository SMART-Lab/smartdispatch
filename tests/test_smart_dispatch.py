import os
import unittest
import tempfile
import shutil

from subprocess import call

from nose.tools import assert_true, assert_equal


class TestSmartdispatcher(unittest.TestCase):

    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()
        self.logs_dir = os.path.join(self.testing_dir, 'SMART_DISPATCH_LOGS')

        base_command = 'smart_dispatch.py --pool 10 -q qtest@mp2 -n 5 -x {0}'
        self.launch_command = base_command.format('launch echo "1 2 3 4" "6 7 8" "9 0"')
        self.resume_command = base_command.format('resume {0}')

        os.chdir(self.testing_dir)

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_main_launch(self):
        # Actual test
        exit_status = call(self.launch_command, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)

    def test_main_resume(self):
        # SetUp
        call(self.launch_command, shell=True)
        batch_uid = os.listdir(self.logs_dir)[0]

        # Actual test
        exit_status = call(self.resume_command.format(batch_uid), shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)
