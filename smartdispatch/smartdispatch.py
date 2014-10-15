from __future__ import absolute_import

import os
import itertools
from datetime import datetime

from smartdispatch import utils

UID_TAG = "{UID}"


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
    '''

    # Suppose `argument`is a space separated list
    return argument.split(" ")


def replace_uid_tag(commands):
    return [command.replace("{UID}", utils.generate_uid_from_string(command)) for command in commands]
