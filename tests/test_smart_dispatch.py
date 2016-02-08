import os
import unittest
import tempfile
import shutil
from os.path import join as pjoin

from subprocess import call

from nose.tools import assert_true, assert_equal


class TestSmartdispatcher(unittest.TestCase):

    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()
        self.logs_dir = os.path.join(self.testing_dir, 'SMART_DISPATCH_LOGS')

        base_command = 'smart_dispatch.py --pool 10 -C 42 -q test -t 5:00 -x {0}'
        self.launch_command = base_command.format('launch echo "[1 2 3 4]" "[6 7 8]" "[9 0]"')
        self.resume_command = base_command.format('resume {0}')

        self._cwd = os.getcwd()
        os.chdir(self.testing_dir)

    def tearDown(self):
        os.chdir(self._cwd)
        shutil.rmtree(self.testing_dir)

    def test_main_launch(self):
        # Actual test
        exit_status = call(self.launch_command, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)

    def test_main_resume(self):
        # Setup
        call(self.launch_command, shell=True)
        batch_uid = os.listdir(self.logs_dir)[0]

        # Simulate that some commands are in the running state.
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        pending_commands_file = pjoin(path_job_commands, "commands.txt")
        running_commands_file = pjoin(path_job_commands, "running_commands.txt")
        commands = open(pending_commands_file).read().strip().split("\n")
        with open(running_commands_file, 'w') as running_commands:
            running_commands.write("\n".join(commands[::2]) + "\n")
        with open(pending_commands_file, 'w') as pending_commands:
            pending_commands.write("\n".join(commands[1::2]) + "\n")

        # Actual test (should move running commands back to pending).
        exit_status = call(self.resume_command.format(batch_uid), shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)
        assert_equal(len(open(running_commands_file).readlines()), 0)
        assert_equal(len(open(pending_commands_file).readlines()), len(commands))

        # Test when batch_uid is a path instead of a jobname.
        # Setup
        batch_uid = os.path.join(self.logs_dir, os.listdir(self.logs_dir)[0])

        # Simulate that some commands are in the running state.
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        pending_commands_file = pjoin(path_job_commands, "commands.txt")
        running_commands_file = pjoin(path_job_commands, "running_commands.txt")
        commands = open(pending_commands_file).read().strip().split("\n")
        with open(running_commands_file, 'w') as running_commands:
            running_commands.write("\n".join(commands[::2]) + "\n")
        with open(pending_commands_file, 'w') as pending_commands:
            pending_commands.write("\n".join(commands[1::2]) + "\n")

        # Actual test (should move running commands back to pending).
        exit_status = call(self.resume_command.format(batch_uid), shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)
        assert_equal(len(open(running_commands_file).readlines()), 0)
        assert_equal(len(open(pending_commands_file).readlines()), len(commands))

    def test_main_resume_only_pending(self):
        # SetUp
        call(self.launch_command, shell=True)
        batch_uid = os.listdir(self.logs_dir)[0]

        # Simulate that some commands are in the running state.
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        pending_commands_file = pjoin(path_job_commands, "commands.txt")
        running_commands_file = pjoin(path_job_commands, "running_commands.txt")
        commands = open(pending_commands_file).read().strip().split("\n")
        with open(running_commands_file, 'w') as running_commands:
            running_commands.write("\n".join(commands[::2]) + "\n")
        with open(pending_commands_file, 'w') as pending_commands:
            pending_commands.write("\n".join(commands[1::2]) + "\n")

        # Actual test (should NOT move running commands back to pending).
        command_line = self.resume_command.format(batch_uid)
        command_line += " --onlyPending"
        exit_status = call(command_line, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)
        assert_equal(len(open(running_commands_file).readlines()), len(commands[::2]))
        assert_equal(len(open(pending_commands_file).readlines()), len(commands[1::2]))
