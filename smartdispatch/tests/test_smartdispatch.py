import smartdispatch
from StringIO import StringIO

from nose.tools import assert_true, assert_equal
from numpy.testing import assert_array_equal
from datetime import datetime
from smartdispatch import utils


def test_generate_name_from_command():
    command = "command arg1 arg2"
    expected = "_".join(command.split())
    assert_equal(smartdispatch.generate_name_from_command(command), expected)

    max_length_arg = 7
    long_arg = "veryverylongarg1"
    command = "command " + long_arg + " arg2"
    expected = command.split()
    expected[1] = long_arg[-max_length_arg:]
    expected = "_".join(expected)
    assert_equal(smartdispatch.generate_name_from_command(command, max_length_arg), expected)

    max_length = 23
    long_arg = "veryverylongarg1"
    command = "command veryverylongarg1 veryverylongarg1 veryverylongarg1 veryverylongarg1"
    expected = command.split()
    expected = "_".join(expected)[-max_length:]
    assert_equal(smartdispatch.generate_name_from_command(command, max_length=max_length), expected)

    # Test path arguments in command
    command = "command path/number/one path/number/two"
    expected = "command_pathnumberone_pathnumbertwo"
    assert_equal(smartdispatch.generate_name_from_command(command), expected)


def test_generate_name_from_arguments():
    prefix = "prefix_"

    arguments = [["my_command"], ["args1a", "args1b", "args1c"], ["args2a", "args2b"]]
    expected = prefix + "my_command_args1a-args1c_args2a-args2b"
    assert_equal(smartdispatch.generate_name_from_arguments(arguments, prefix=prefix), expected)

    max_length_arg = 7
    arguments = [["command"], ["verylongargs1a", "verylongargs1b", "verylongargs1c"], ["args2a", "args2b"]]
    expected = prefix + "command_" + arguments[1][0][-max_length_arg:] + "-" + arguments[1][-1][-max_length_arg:] + "_args2a-args2b"
    assert_equal(smartdispatch.generate_name_from_arguments(arguments, max_length_arg, prefix=prefix), expected)

    max_length = 23
    arguments = [["command"], ["verylongargs1a", "verylongargs1b", "verylongargs1c"], ["args2a", "args2b"]]
    expected = "command_" + arguments[1][0] + "-" + arguments[1][-1] + "_args2a-args2b"
    expected = prefix + expected[-max_length:]
    assert_equal(smartdispatch.generate_name_from_arguments(arguments, max_length=max_length, prefix=prefix), expected)

    # Test path arguments in command
    arguments = [["command"], ["path/argument/1", "path/argument/2", "path/argument/3"]]
    expected = prefix + "command_pathargument1-pathargument3"
    assert_equal(smartdispatch.generate_name_from_arguments(arguments, prefix=prefix), expected)

    # Make sure default prefix does not raise exception
    arguments = [["command"]]
    results = smartdispatch.generate_name_from_arguments(arguments)
    expect_datetime = datetime.now()
    assert_equal(results.split("_")[-1], arguments[0][0])
    result_datetime = datetime.strptime("_".join(results.split("_")[:-1]), '%Y-%m-%d_%H-%M-%S')
    assert_true(result_datetime <= expect_datetime)


def test_get_commands_from_file():
    commands = ["command1 arg1 arg2",
                "command2",
                "command3 arg1 arg2 arg3 arg4"]
    fileobj = StringIO("\n".join(commands))
    assert_array_equal(smartdispatch.get_commands_from_file(fileobj), commands)

    # Test stripping last line if empty
    fileobj = StringIO("\n".join(commands) + "\n")
    assert_array_equal(smartdispatch.get_commands_from_file(fileobj), commands)


def test_get_commands_from_arguments():
    # Test single unfolded arguments
    args = "ls"
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["ls"])

    args = "echo 1"
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["echo 1"])

    args = "echo [1 2]"
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["echo 1", "echo 2"])

    # Test multiple unfolded arguments
    args = "python my_command.py [0.01 0.000001 0.00000000001] -1 [omicron mu]"
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["python my_command.py 0.01 -1 omicron",
                                                                   "python my_command.py 0.01 -1 mu",
                                                                   "python my_command.py 0.000001 -1 omicron",
                                                                   "python my_command.py 0.000001 -1 mu",
                                                                   "python my_command.py 0.00000000001 -1 omicron",
                                                                   "python my_command.py 0.00000000001 -1 mu"])

    # Test multiple unfolded arguments and not unfoldable brackets
    args = "python my_command.py [0.01 0.000001 0.00000000001] -1 [[42 133,666]] slow [omicron mu]"
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["python my_command.py 0.01 -1 [42] slow omicron",
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


def test_unfold_argument():
    # Test simple argument
    for arg in ["arg1", "[arg1"]:
        assert_array_equal(smartdispatch.unfold_argument(arg), [arg])

    # Test list (space)
    for arg in ["arg1 arg2", "arg1 ", " arg1"]:
        assert_array_equal(smartdispatch.unfold_argument(arg), arg.split(" "))


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
    assert_array_equal(smartdispatch.replace_uid_tag(commands), [commands[0].replace("{UID}", uid)]*len(commands))


def test_get_available_queues():
    assert_equal(smartdispatch.get_available_queues(cluster_name=None), {})
    assert_equal(smartdispatch.get_available_queues(cluster_name="unknown"), {})

    queues_infos = smartdispatch.get_available_queues(cluster_name="guillimin")
    assert_true(len(queues_infos) > 0)

    queues_infos = smartdispatch.get_available_queues(cluster_name="mammouth")
    assert_true(len(queues_infos) > 0)
