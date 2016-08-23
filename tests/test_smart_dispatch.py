import os
import unittest
import tempfile
import shutil
from os.path import join as pjoin, abspath

from subprocess import call

from nose.tools import assert_true, assert_equal


class TestSmartdispatcher(unittest.TestCase):

    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()
        self.logs_dir = os.path.join(self.testing_dir, 'SMART_DISPATCH_LOGS')

        self.folded_commands = 'echo "[1 2 3 4]" "[6 7 8]" "[9 0]"'
        self.commands = ["echo 1 6 9", "echo 1 6 0", "echo 1 7 9", "echo 1 7 0", "echo 1 8 9", "echo 1 8 0",
                         "echo 2 6 9", "echo 2 6 0", "echo 2 7 9", "echo 2 7 0", "echo 2 8 9", "echo 2 8 0",
                         "echo 3 6 9", "echo 3 6 0", "echo 3 7 9", "echo 3 7 0", "echo 3 8 9", "echo 3 8 0",
                         "echo 4 6 9", "echo 4 6 0", "echo 4 7 9", "echo 4 7 0", "echo 4 8 9", "echo 4 8 0"]
        self.nb_commands = len(self.commands)

        scripts_path = abspath(pjoin(os.path.dirname(__file__), os.pardir, "scripts"))
        self.smart_dispatch_command = '{} -C 1 -q test -t 5:00 -x'.format(pjoin(scripts_path, 'smart-dispatch'))
        self.launch_command = "{0} launch {1}".format(self.smart_dispatch_command, self.folded_commands)
        self.resume_command = "{0} resume {{0}}".format(self.smart_dispatch_command)

        smart_dispatch_command_with_pool = '{} --pool 10 -C 1 -q test -t 5:00 -x {{0}}'.format(pjoin(scripts_path, 'smart-dispatch'))
        self.launch_command_with_pool = smart_dispatch_command_with_pool.format('launch ' + self.folded_commands)
        self.nb_workers = 10

        smart_dispatch_command_with_cores = '{} -C 1 -c {{cores}} -q test -t 5:00 -x {{0}}'.format(pjoin(scripts_path, 'smart-dispatch'))
        self.launch_command_with_cores = smart_dispatch_command_with_cores.format('launch ' + self.folded_commands, cores='{cores}')

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

    def test_launch_using_commands_file(self):
        # Actual test
        commands_filename = "commands_to_run.txt"
        open(commands_filename, 'w').write("\n".join(self.commands))

        launch_command = self.smart_dispatch_command + " -f {0} launch".format(commands_filename)
        exit_status = call(launch_command, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_true(os.path.isdir(self.logs_dir))
        assert_equal(len(os.listdir(self.logs_dir)), 1)

        batch_uid = os.listdir(self.logs_dir)[0]
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        assert_equal(len(os.listdir(path_job_commands)), self.nb_commands + 1)
        assert_equal(open(pjoin(path_job_commands, 'commands.txt')).read(), "\n".join(self.commands) + "\n")

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

    def test_main_launch_with_cores_command(self):
        # Actual test
        exit_status_0 = call(self.launch_command_with_cores.format(cores=0), shell=True)
        exit_status_100 = call(self.launch_command_with_cores.format(cores=100), shell=True)

        # Test validation
        assert_equal(exit_status_0, 2)
        assert_equal(exit_status_100, 2)        
        assert_true(os.path.isdir(self.logs_dir))

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

    def test_main_resume_by_expanding_pool_default(self):
        # Create SMART_DISPATCH_LOGS structure.
        call(self.launch_command, shell=True)
        batch_uid = os.listdir(self.logs_dir)[0]

        # Simulate that some commands are in the running state.
        nb_commands_files = 2  # 'commands.txt' and 'running_commands.txt'
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        pending_commands_file = pjoin(path_job_commands, "commands.txt")
        running_commands_file = pjoin(path_job_commands, "running_commands.txt")
        commands = open(pending_commands_file).read().strip().split("\n")
        with open(running_commands_file, 'w') as running_commands:
            running_commands.write("\n".join(commands[::2]) + "\n")
        with open(pending_commands_file, 'w') as pending_commands:
            pending_commands.write("\n".join(commands[1::2]) + "\n")

        # Remove PBS files so we can check that new ones are going to be created.
        for f in os.listdir(path_job_commands):
            if f.startswith('job_commands_') and f.endswith('.sh'):
                os.remove(pjoin(path_job_commands, f))

        # Should NOT move running commands back to pending but should add new workers.
        command_line = self.resume_command.format(batch_uid)
        command_line += " --expandPool"
        exit_status = call(command_line, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_equal(len(open(running_commands_file).readlines()), len(commands[::2]))
        assert_equal(len(open(pending_commands_file).readlines()), len(commands[1::2]))

        nb_job_commands_files = len(os.listdir(path_job_commands))
        assert_equal(nb_job_commands_files-nb_commands_files, len(commands[1::2]))

    def test_main_resume_by_expanding_pool(self):
        # Create SMART_DISPATCH_LOGS structure.
        call(self.launch_command, shell=True)
        batch_uid = os.listdir(self.logs_dir)[0]

        # Simulate that some commands are in the running state.
        nb_commands_files = 2  # 'commands.txt' and 'running_commands.txt'
        path_job_commands = os.path.join(self.logs_dir, batch_uid, "commands")
        pending_commands_file = pjoin(path_job_commands, "commands.txt")
        running_commands_file = pjoin(path_job_commands, "running_commands.txt")
        commands = open(pending_commands_file).read().strip().split("\n")
        with open(running_commands_file, 'w') as running_commands:
            running_commands.write("\n".join(commands[::2]) + "\n")
        with open(pending_commands_file, 'w') as pending_commands:
            pending_commands.write("\n".join(commands[1::2]) + "\n")

        # Remove PBS files so we can check that new ones are going to be created.
        for f in os.listdir(path_job_commands):
            if f.startswith('job_commands_') and f.endswith('.sh'):
                os.remove(pjoin(path_job_commands, f))

        # Should NOT move running commands back to pending but should add new workers.
        nb_workers_to_add = 3
        command_line = self.resume_command.format(batch_uid)
        command_line += " --expandPool {}".format(nb_workers_to_add)
        exit_status = call(command_line, shell=True)

        # Test validation
        assert_equal(exit_status, 0)
        assert_equal(len(open(running_commands_file).readlines()), len(commands[::2]))
        assert_equal(len(open(pending_commands_file).readlines()), len(commands[1::2]))

        nb_job_commands_files = len(os.listdir(path_job_commands))
        assert_equal(nb_job_commands_files-nb_commands_files, nb_workers_to_add)
