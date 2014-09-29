import smartdispatch
from StringIO import StringIO

from nose.tools import assert_true, assert_equal
from numpy.testing import assert_array_equal
from datetime import datetime


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


def test_get_commands_from_arguments():
    # Test single unfolded arguments
    args = [["arg"]]
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["arg"])

    args = [["args1_a", "args1_b"]]
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["args1_a", "args1_b"])

    # Test multiple unfolded arguments
    args = [["args1"], ["args2"]]
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["args1 args2"])

    args = [["args1_a", "args1_b", "args1_c"], ["args2_a", "args2_b"]]
    assert_equal(smartdispatch.get_commands_from_arguments(args), ["args1_a args2_a", "args1_a args2_b",
                                                                   "args1_b args2_a", "args1_b args2_b",
                                                                   "args1_c args2_a", "args1_c args2_b"])


def test_unfold_argument():
    # Test simple argument
    for arg in ["arg1", "[arg1"]:
        assert_array_equal(smartdispatch.unfold_argument(arg), [arg])

    # Test list (space)
    for arg in ["arg1 arg2", "arg1 ", " arg1"]:
        assert_array_equal(smartdispatch.unfold_argument(arg), arg.split(" "))

    # Test list (comma)
    for arg in ["[arg1 arg2]", "[subarg11 subarg12, arg2]", "[arg1, arg2]", "[arg1, ]", "[ ,arg1]"]:
        assert_array_equal(smartdispatch.unfold_argument(arg), arg[1:-1].split(","))


def test_generate_pbs():
    # Create empty PBS file
    expected = """#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -l walltime=01:00:00
#PBS -l nodes=1:ppn=1

# Modules #

# Commands #

wait"""
    pbs = smartdispatch.generate_pbs([], queue="qtest@mp2", walltime="01:00:00")
    assert_equal(pbs, expected)

    # Test options
    expected = """#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -A xyz-123-ab
#PBS -l walltime=01:00:00
#PBS -l nodes=2:ppn=3:gpus=1

# Modules #

# Commands #

wait"""
    kwargs = dict(account_name="xyz-123-ab", nodes=2, ppn=3, gpus=1)
    pbs = smartdispatch.generate_pbs([], queue="qtest@mp2", walltime="01:00:00", **kwargs)
    assert_equal(pbs, expected)

    # Test modules
    expected = """#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -l walltime=01:00:00
#PBS -l nodes=1:ppn=1

# Modules #
module load CUDA_Toolkit/6.0
module load python2.7

# Commands #

wait"""
    kwargs = dict(modules=["CUDA_Toolkit/6.0", "python2.7"])
    pbs = smartdispatch.generate_pbs([], queue="qtest@mp2", walltime="01:00:00", **kwargs)
    assert_equal(pbs, expected)

    # Test commands
    commands = ["echo 1 2 3", "echo 3 2 1"]
    names = map(smartdispatch.generate_name_from_command, commands)
    expected = """#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -l walltime=01:00:00
#PBS -l nodes=1:ppn=1

# Modules #

# Commands #
cd .; {command1} 1>> "./{name1}.o" 2>> "./{name1}.e" &
cd .; {command2} 1>> "./{name2}.o" 2>> "./{name2}.e" &

wait""".format(command1=commands[0], command2=commands[1],
               name1=names[0], name2=names[1])
    pbs = smartdispatch.generate_pbs(commands, queue="qtest@mp2", walltime="01:00:00")
    assert_equal(pbs, expected)

    # Test cwd and logs_dir
    cwd = "/path/to/cwd"
    logs_dir = "/path/to/logs_dir/"
    commands = ["echo 1 2 3"]
    names = map(smartdispatch.generate_name_from_command, commands)
    expected = """#!/bin/bash
#PBS -q qtest@mp2
#PBS -V
#PBS -l walltime=01:00:00
#PBS -l nodes=1:ppn=1

# Modules #

# Commands #
cd {cwd}; {command} 1>> "{logs_dir}{name}.o" 2>> "{logs_dir}{name}.e" &

wait""".format(command=commands[0], name=names[0], cwd=cwd, logs_dir=logs_dir)
    pbs = smartdispatch.generate_pbs(commands, queue="qtest@mp2", walltime="01:00:00", cwd=cwd, logs_dir=logs_dir)
    assert_equal(pbs, expected)
