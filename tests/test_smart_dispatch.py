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

        self.commands = 'echo "[1 2 3 4]" "[6 7 8]" "[9 0]"'
        self.nb_commands = 4*3*2

        smart_dispatch_command = 'smart-dispatch -C 1 -q test -t 5:00 -x {0}'
        self.launch_command = smart_dispatch_command.format('launch ' + self.commands)
        self.resume_command = smart_dispatch_command.format('resume {0}')

        smart_dispatch_command_with_pool = 'smart-dispatch --pool 10 -C 1 -q test -t 5:00 -x {0}'
        self.launch_command_with_pool = smart_dispatch_command_with_pool.format('launch ' + self.commands)
        self.nb_workers = 10

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

        batch_uid = os.listdir(self.logs_dir)[0]
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        assert_equal(len(os.listdir(path_job_commands)), self.nb_commands + 1)

    def test_main_launch_with_pool_of_workers(self):
        # Actual test
        exit_status = call(self.launch_command_with_pool, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)

        batch_uid = os.listdir(self.logs_dir)[0]
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        assert_equal(len(os.listdir(path_job_commands)), self.nb_workers + 1)

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
