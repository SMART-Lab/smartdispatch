import os
import re
import shutil
import time as t
from os.path import join as pjoin
from StringIO import StringIO

import tempfile
from nose.tools import assert_true, assert_equal
from numpy.testing import assert_array_equal

import smartdispatch
from smartdispatch import utils


def test_generate_name_from_command():
    date_length = 20

    command = "command arg1 arg2"
    expected = "_".join(command.split())
    assert_equal(smartdispatch.generate_name_from_command(command)[date_length:], expected)

    max_length_arg = 7
    long_arg = "veryverylongarg1"
    command = "command " + long_arg + " arg2"
    expected = command.split()
    expected[1] = long_arg[-max_length_arg:]
    expected = "_".join(expected)
    assert_equal(smartdispatch.generate_name_from_command(command, max_length_arg)[date_length:], expected)

    max_length = 23
    command = "command veryverylongarg1 veryverylongarg1 veryverylongarg1 veryverylongarg1"
    expected = command[:max_length].replace(" ", "_")
    assert_equal(smartdispatch.generate_name_from_command(command, max_length=max_length + date_length)[date_length:], expected)

    # Test path arguments in command
    command = "command path/number/one path/number/two"
    expected = "command_pathnumberone_pathnumbertwo"
    assert_equal(smartdispatch.generate_name_from_command(command)[date_length:], expected)


def test_get_commands_from_file():
    commands = ["command1 arg1 arg2",
                "command2",
                "command3 arg1 arg2 arg3 arg4"]
    fileobj = StringIO("\n".join(commands))
    assert_array_equal(smartdispatch.get_commands_from_file(fileobj), commands)

    # Test stripping last line if empty
    fileobj = StringIO("\n".join(commands) + "\n")
    assert_array_equal(smartdispatch.get_commands_from_file(fileobj), commands)


def test_unfold_command():
    # Test with one argument
    cmd = "ls"
    assert_equal(smartdispatch.unfold_command(cmd), ["ls"])

    cmd = "echo 1"
    assert_equal(smartdispatch.unfold_command(cmd), ["echo 1"])

    # Test two arguments
    cmd = "echo [1 2]"
    assert_equal(smartdispatch.unfold_command(cmd), ["echo 1", "echo 2"])

    cmd = "echo test [1 2] yay"
    assert_equal(smartdispatch.unfold_command(cmd), ["echo test 1 yay", "echo test 2 yay"])

    cmd = "echo test[1 2]"
    assert_equal(smartdispatch.unfold_command(cmd), ["echo test1", "echo test2"])

    cmd = "echo test[1 2]yay"
    assert_equal(smartdispatch.unfold_command(cmd), ["echo test1yay", "echo test2yay"])

    # Test multiple folded arguments
    cmd = "python my_command.py [0.01 0.000001 0.00000000001] -1 [omicron mu]"
    assert_equal(smartdispatch.unfold_command(cmd), ["python my_command.py 0.01 -1 omicron",
                                                     "python my_command.py 0.01 -1 mu",
                                                     "python my_command.py 0.000001 -1 omicron",
                                                     "python my_command.py 0.000001 -1 mu",
                                                     "python my_command.py 0.00000000001 -1 omicron",
                                                     "python my_command.py 0.00000000001 -1 mu"])

    # Test multiple folded arguments and not unfoldable brackets
    cmd = "python my_command.py [0.01 0.000001 0.00000000001] -1 \[[42 133,666]\] slow [omicron mu]"
    assert_equal(smartdispatch.unfold_command(cmd), ["python my_command.py 0.01 -1 [42] slow omicron",
                                                     "python my_command.py 0.01 -1 [42] slow mu",
                                                     "python my_command.py 0.01 -1 [133,666] slow omicron",
                                                     "python my_command.py 0.01 -1 [133,666] slow mu",
                                                     "python my_command.py 0.000001 -1 [42] slow omicron",
                                                     "python my_command.py 0.000001 -1 [42] slow mu",
                                                     "python my_command.py 0.000001 -1 [133,666] slow omicron",
                                                     "python my_command.py 0.000001 -1 [133,666] slow mu",
                                                     "python my_command.py 0.00000000001 -1 [42] slow omicron",
                                                     "python my_command.py 0.00000000001 -1 [42] slow mu",
                                                     "python my_command.py 0.00000000001 -1 [133,666] slow omicron",
                                                     "python my_command.py 0.00000000001 -1 [133,666] slow mu"])

    # Test multiple different folded arguments
    cmd = "python my_command.py [0.01 0.001] -[1:5] slow"
    assert_equal(smartdispatch.unfold_command(cmd), ["python my_command.py 0.01 -1 slow",
                                                     "python my_command.py 0.01 -2 slow",
                                                     "python my_command.py 0.01 -3 slow",
                                                     "python my_command.py 0.01 -4 slow",
                                                     "python my_command.py 0.001 -1 slow",
                                                     "python my_command.py 0.001 -2 slow",
                                                     "python my_command.py 0.001 -3 slow",
                                                     "python my_command.py 0.001 -4 slow"])

    cmd = "python my_command.py -[1:5] slow [0.01 0.001]"
    assert_equal(smartdispatch.unfold_command(cmd), ["python my_command.py -1 slow 0.01",
                                                     "python my_command.py -1 slow 0.001",
                                                     "python my_command.py -2 slow 0.01",
                                                     "python my_command.py -2 slow 0.001",
                                                     "python my_command.py -3 slow 0.01",
                                                     "python my_command.py -3 slow 0.001",
                                                     "python my_command.py -4 slow 0.01",
                                                     "python my_command.py -4 slow 0.001"])


def test_replace_uid_tag():
    command = "command without uid tag"
    assert_array_equal(smartdispatch.replace_uid_tag([command]), [command])

    command = "command with one {UID} tag"
    uid = utils.generate_uid_from_string(command)
    assert_array_equal(smartdispatch.replace_uid_tag([command]), [command.replace("{UID}", uid)])

    command = "command with two {UID} tag {UID}"
    uid = utils.generate_uid_from_string(command)
    assert_array_equal(smartdispatch.replace_uid_tag([command]), [command.replace("{UID}", uid)])

    commands = ["a command with a {UID} tag"] * 10
    uid = utils.generate_uid_from_string(commands[0])
    assert_array_equal(smartdispatch.replace_uid_tag(commands), [commands[0].replace("{UID}", uid)] * len(commands))


def test_get_available_queues():
    assert_equal(smartdispatch.get_available_queues(cluster_name=None), {})
    assert_equal(smartdispatch.get_available_queues(cluster_name="unknown"), {})

    queues_infos = smartdispatch.get_available_queues(cluster_name="guillimin")
    assert_true(len(queues_infos) > 0)

    queues_infos = smartdispatch.get_available_queues(cluster_name="mammouth")
    assert_true(len(queues_infos) > 0)


def test_get_job_folders():
    temp_dir = tempfile.mkdtemp()
    jobname = "this_is_the_name_of_my_job"
    job_folders_paths = smartdispatch.get_job_folders(temp_dir, jobname)
    path_job, path_job_logs, path_job_commands = job_folders_paths

    assert_true(jobname in path_job)
    assert_true(os.path.isdir(path_job))
    assert_equal(os.path.basename(path_job), jobname)

    assert_true(jobname in path_job_logs)
    assert_true(os.path.isdir(path_job_logs))
    assert_true(os.path.isdir(pjoin(path_job_logs, 'worker')))
    assert_true(os.path.isdir(pjoin(path_job_logs, 'job')))
    assert_true(os.path.isdir(path_job_logs))
    assert_equal(os.path.basename(path_job_logs), "logs")

    assert_true(jobname in path_job_commands)
    assert_true(os.path.isdir(path_job_commands))
    assert_equal(os.path.basename(path_job_commands), "commands")

    # In theory the following should not create new folders.
    # Insteead it will return the paths to existing folders.
    jobname += "2"
    os.rename(path_job, path_job + "2")
    job_folders_paths = smartdispatch.get_job_folders(temp_dir, jobname)
    path_job, path_job_logs, path_job_commands = job_folders_paths

    assert_true(jobname in path_job)
    assert_true(os.path.isdir(path_job))
    assert_equal(os.path.basename(path_job), jobname)

    assert_true(jobname in path_job_logs)
    assert_true(os.path.isdir(path_job_logs))
    assert_true(os.path.isdir(pjoin(path_job_logs, 'worker')))
    assert_true(os.path.isdir(pjoin(path_job_logs, 'job')))
    assert_true(os.path.isdir(path_job_logs))
    assert_equal(os.path.basename(path_job_logs), "logs")

    assert_true(jobname in path_job_commands)
    assert_true(os.path.isdir(path_job_commands))
    assert_equal(os.path.basename(path_job_commands), "commands")

    shutil.rmtree(temp_dir)


def test_log_command_line():
    temp_dir = tempfile.mkdtemp()
    command_line_log_file = pjoin(temp_dir, "command_line.log")

    command_1 = "echo 1 2 3"
    smartdispatch.log_command_line(temp_dir, command_1)
    assert_true(os.path.isfile(command_line_log_file))

    lines = open(command_line_log_file).read().strip().split("\n")
    assert_equal(len(lines), 2)  # Datetime, the command line.
    assert_true(t.strftime("## %Y-%m-%d %H:%M:") in lines[0])  # Don't check second.
    assert_equal(lines[1], command_1)

    command_2 = "echo \"bob\""  # With quotes.
    smartdispatch.log_command_line(temp_dir, command_2)
    assert_true(os.path.isfile(command_line_log_file))

    lines = open(command_line_log_file).read().strip().split("\n")
    assert_equal(len(lines), 5)
    assert_true(t.strftime("## %Y-%m-%d %H:%M:") in lines[3])  # Don't check second.
    assert_equal(lines[4], command_2.replace('"', r'\"'))

    command_3 = "echo [asd]"  # With square brackets.
    smartdispatch.log_command_line(temp_dir, command_3)
    assert_true(os.path.isfile(command_line_log_file))

    lines = open(command_line_log_file).read().strip().split("\n")
    assert_equal(len(lines), 8)
    assert_true(t.strftime("## %Y-%m-%d %H:%M:") in lines[6])  # Don't check second.
    assert_equal(lines[7], re.sub(r'(\[)([^\[\]]*\\ [^\[\]]*)(\])', r'"\1\2\3"', command_3))

    shutil.rmtree(temp_dir)
