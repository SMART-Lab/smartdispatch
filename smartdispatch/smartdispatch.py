from __future__ import absolute_import

import os
import re
import itertools
import time as t
from os.path import join as pjoin
from subprocess import check_output

import smartdispatch
from smartdispatch import utils
from smartdispatch.filelock import open_with_lock
from smartdispatch.argument_template import argument_templates

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

    return generate_logfolder_name('_'.join([utils.slugify(argvalue)[max_length_arg:] for argvalue in command.split()]), max_length)


def generate_logfolder_name(name, max_length=None):
    folder_name = t.strftime("%Y-%m-%d_%H-%M-%S_")
    folder_name += name
    return folder_name[:max_length]


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

    return  [line.strip() for line in fileobj if len(line.strip()) > 0]


def unfold_command(command):
    ''' Unfolds a command into a list of unfolded commands.

    Unfolding is performed for every folded arguments (see *Arguments templates*)
    found in `command`. Then, resulting commands are generated using the product
    of every unfolded arguments.

    Parameters
    ----------
    command : list of str
        command to unfold

    Returns
    -------
    commands : list of str
        commands obtained after unfolding `command`

    Arguments template
    ------------------
    *list*: "[item1 item2 ... itemN]"
    *range*: "[start:end]" or "[start:end:step]"
    '''
    text = utils.encode_escaped_characters(command)

    # Build the master regex with all argument's regex
    regex = "(" + "|".join(["(?P<{0}>{1})".format(name, arg.regex) for name, arg in argument_templates.items()]) + ")"

    pos = 0
    arguments = []
    for match in re.finditer(regex, text):
        # Add already unfolded argument
        arguments.append([text[pos:match.start()]])

        # Unfold argument
        argument_template_name, matched_text = next((k, v) for k, v in match.groupdict().items() if v is not None)
        arguments.append(argument_templates[argument_template_name].unfold(matched_text))
        pos = match.end()

    arguments.append([text[pos:]])  # Add remaining unfolded arguments
    arguments = [map(utils.decode_escaped_characters, argvalues) for argvalues in arguments]
    return ["".join(argvalues) for argvalues in itertools.product(*arguments)]


def replace_uid_tag(commands):
    return [command.replace("{UID}", utils.generate_uid_from_string(command)) for command in commands]


def get_available_queues(cluster_name):
    """ Fetches all available queues on the current cluster """
    if cluster_name is None:
        return {}

    smartdispatch_dir, _ = os.path.split(smartdispatch.__file__)
    config_dir = pjoin(smartdispatch_dir, 'config')

    config_filename = cluster_name + ".json"
    config_filepath = pjoin(config_dir, config_filename)

    if not os.path.isfile(config_filepath):
        return {}  # Unknown cluster

    queues_infos = utils.load_dict_from_json_file(config_filepath)
    return queues_infos


def get_job_folders(path, jobname, create_if_needed=False):
    """ Get all folder paths for a specific job (creating them if needed). """
    path_job = pjoin(path, jobname)
    path_job_logs = pjoin(path_job, 'logs')
    path_job_commands = pjoin(path_job, 'commands')

    if not os.path.isdir(path_job_commands):
        os.makedirs(path_job_commands)
    if not os.path.isdir(path_job_logs):
        os.makedirs(path_job_logs)
    if not os.path.isdir(pjoin(path_job_logs, "worker")):
        os.makedirs(pjoin(path_job_logs, "worker"))
    if not os.path.isdir(pjoin(path_job_logs, "job")):
        os.makedirs(pjoin(path_job_logs, "job"))

    return path_job, path_job_logs, path_job_commands


def log_command_line(path_job, command_line):
    """ Logs a command line in a job folder.

    The command line is append to a file named 'command_line.log' that resides
    in the given job folder. The current date and time is also added along
    each command line logged.

    Notes
    -----
    Commands save in log file might differ from sys.argv since we want to make sure
    we can paste the command line as-is in the terminal. This means that the quotes
    symbole " and the square brackets will be escaped.
    """
    with open_with_lock(pjoin(path_job, "command_line.log"), 'a') as command_line_log:
        command_line_log.write(t.strftime("## %Y-%m-%d %H:%M:%S ##\n"))
        command_line = command_line.replace('"', r'\"')  # Make sure we can paste the command line as-is
        command_line = re.sub(r'(\[)([^\[\]]*\\ [^\[\]]*)(\])', r'"\1\2\3"', command_line)  # Make sure we can paste the command line as-is
        command_line_log.write(command_line + "\n\n")


def launch_jobs(launcher, pbs_filenames, cluster_name, path_job):  # pragma: no cover
    ''' Invokes launcher on a set of PBS files.

    Parameters
    ----------
    launcher : str
        launcher name
    pbs_filenames : list of str
        a list of PBS files to launch
    cluster_name : str
        cluster name
    path_job : str
        path to the job folder
    '''
    jobs_id = []
    for pbs_filename in pbs_filenames:
        launcher_output = check_output('PBS_FILENAME={pbs_filename} {launcher} {pbs_filename}'.format(
            launcher=launcher, pbs_filename=pbs_filename), shell=True)
        jobs_id += [launcher_output.strip()]

        # On some clusters, SRMJID and PBS_JOBID don't match
        if cluster_name in ['helios']:
            launcher_output = check_output(['qstat', '-f']).split('Job Id: ')
            for job in launcher_output:
                if re.search(r"SRMJID:{job_id}".format(job_id=jobs_id[-1]), job):
                    pbs_job_id = re.match(r"[0-9a-zA-Z.-]*", job).group()
                    jobs_id[-1] = '{pbs}'.format(pbs=pbs_job_id)

    with open_with_lock(pjoin(path_job, "jobs_id.txt"), 'a') as jobs_id_file:
        jobs_id_file.writelines(t.strftime("## %Y-%m-%d %H:%M:%S ##\n"))
        jobs_id_file.writelines("\n".join(jobs_id) + "\n")
    print "\nJobs id:\n{jobs_id}".format(jobs_id=" ".join(jobs_id))
