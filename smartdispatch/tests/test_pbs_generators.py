import smartdispatch
from nose.tools import assert_equal


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
