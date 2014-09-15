from __future__ import absolute_import

import os
import itertools
from datetime import datetime

import smartdispatch.utils as utils


def generate_name_from_command(command, max_length_arg=None, max_length=None):
    ''' Generates name from a given command.

    Generate a name by replacing spaces in command with dashes and
    by trimming lengthty (as defined by max_length_arg) arguments.

    Parameters
    ----------
    command : str
        command from which to generate the name
    max_length_arg : int
        arguments longer than this will be trimmed keeping last characters (Default: inf)
    max_length : int
        trim name if longer than this keeping last characters (Default: inf)

    Returns
    -------
    name : str
        slugified name
    '''
    if max_length_arg is not None:
        max_length_arg = min(-max_length_arg, max_length_arg)

    if max_length is not None:
        max_length = min(-max_length, max_length)

    name = '_'.join([utils.slugify(argvalue)[max_length_arg:] for argvalue in command.split()])
    return name[max_length:]


def generate_name_from_arguments(arguments, max_length_arg=None, max_length=None, prefix=datetime.now().strftime('%Y-%m-%d_%H-%M-%S_')):
    ''' Generates name from given unfolded arguments.

    Generate a name by concatenating the first and last values of every
    unfolded arguments and by trimming lengthty (as defined by max_length_arg)
    arguments.

    Parameters
    ----------
    arguments : list of list of str
        list of unfolded arguments
    max_length_arg : int
        arguments longer than this will be trimmed keeping last characters (Default: inf)
    max_length : int
        trim name if longer than this keeping last characters (Default: inf)
    prefix : str
        text to preprend to the name (Default: current datetime)

    Returns
    -------
    name : str
        slugified name
    '''
    if max_length_arg is not None:
        max_length_arg = min(-max_length_arg, max_length_arg)

    if max_length is not None:
        max_length = min(-max_length, max_length)

    name = []
    for argvalues in arguments:
        argvalues = map(utils.slugify, argvalues)
        name.append(argvalues[0][max_length_arg:])
        if len(argvalues) > 1:
            name[-1] += '-' + argvalues[-1][max_length_arg:]

    name = "_".join(name)

    name = prefix + name[max_length:]
    return name


def get_commands_from_file(fileobj):
    ''' Reads commands from `fileobj`.

    Parameters
    ----------
    fileobj : file
        opened file where to read commands from

    Returns
    -------
    commands : list of str
        commands read from the file
    '''
    return fileobj.read().split('\n')


def get_commands_from_arguments(arguments):
    ''' Obtains commands from the product of every unfolded arguments.

    Parameters
    ----------
    arguments : list of list of str
        list of unfolded arguments

    Returns
    -------
    commands : list of str
        commands resulting from the product of every unfolded arguments
    '''
    return [" ".join(argvalues) for argvalues in itertools.product(*arguments)]


def unfold_argument(argument):
    ''' Unfolds a folded argument into a list of unfolded arguments.

    An argument can be folded e.g. a list of unfolded arguments separated by spaces.
    An unfolded argument unfolds to itself.

    Parameters
    ----------
    argument : str
        argument to unfold

    Returns
    -------
    unfolded_arguments : list of str
        result of the unfolding

    Complex arguments
    -----------------
    *list (space)*: "item1 item2 ... itemN"
    *list (comma)*: "[item1,item2,...,itemN]"
    '''

    # Check if `argument` is a comma separated list (may contain spaces)
    if argument[0] == "[" and argument[-1] == "]":
        return argument[1:-1].split(",")

    # Suppose `argument`is a space separated list
    return argument.split(" ")


def generate_pbs(commands, queue, walltime, cwd='.', logs_dir='.', **kwargs):
    ''' Generates the content of a PBS file used by the command qsub.

    Parameters
    ----------
    commands : list of str
        list of every commands to be included in the PBS file
    queue : str
        name of the queue on which commands will be excuted
    walltime : str
        maximum time allocated to execute every commands (DD:HH:MM:SS)
    cwd : str
        current working directory where commands will be executed
    logs_dir : str
        directory where commands' logs will be saved
    kwargs : dictionnary of options
        options to add in the PBS (see below for supported options)

    Returns
    -------
    pbs : str
        content of the PBS file

    Options
    -------
    *account_name*:
        name of the account to use (usally RAPid)
    *nodes*
        number of nodes needed (default: 1)
    *ppn*
        number of cpus needed (default: 1)
    *gpus*
        number of gpus needed (default: 0)
    *modules*
        list of modules to load prior executing commands
    '''

    pbs = []
    pbs += ["#!/bin/bash"]
    pbs += ["#PBS -q " + queue]
    pbs += ["#PBS -V"]

    if "account_name" in kwargs:
        pbs += ["#PBS -A " + kwargs['account_name']]

    pbs += ["#PBS -l walltime=" + walltime]
    pbs += ["#PBS -l nodes={nodes}:ppn={ppn}".format(nodes=kwargs.get('nodes', 1), ppn=kwargs.get('ppn', 1))]

    if "gpus" in kwargs:
        pbs[-1] += ":gpus=" + str(kwargs['gpus'])

    pbs += ["\n# Modules #"]

    for module in kwargs.get('modules', []):
        pbs += ["module load " + module]

    pbs += ["\n# Commands #"]

    command_template = 'cd ' + cwd + '; {command} 1>> "{output_log}" 2>> "{error_log}" &'
    for command in commands:
        log_filename = os.path.join(logs_dir, generate_name_from_command(command))
        pbs += [command_template.format(command=command,
                                        output_log=log_filename+".o",
                                        error_log=log_filename+".e")]

    pbs += ["\nwait"]

    return "\n".join(pbs)
