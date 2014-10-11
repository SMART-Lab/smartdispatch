from __future__ import absolute_import

import os
import os.path as path
import logging

import smartdispatch
from smartdispatch.pbs import PBS
from smartdispatch import utils


def get_known_queues():
    """ Fetches all known queues from config files """
    smartdispatch_dir, _ = path.split(smartdispatch.__file__)
    config_dir = path.join(smartdispatch_dir, 'config')

    known_queues = {}
    for config_filename in os.listdir(config_dir):
        cluster_name = path.splitext(config_filename)[0]
        config_filepath = path.join(config_dir, config_filename)
        queues_infos = utils.load_dict_from_json_file(config_filepath)
        for name, infos in queues_infos.items():
            infos['cluster_name'] = cluster_name
            known_queues[name] = infos

    return known_queues


def queue_factory(name, walltime=None, cores=None, gpus=None, modules=None):
    known_queues = get_known_queues()

    cluster_name = utils.detect_cluster()
    if name in known_queues:
        queue_infos = known_queues[name]
        cluster_name = queue_infos['cluster_name']

        if walltime is None:
            walltime = queue_infos['max_walltime']
        if cores is None:
            cores = queue_infos['cores']
        if gpus is None:
            gpus = queue_infos.get('gpus', 0)
        if modules is None:
            modules = queue_infos.get('modules', [])

    if cluster_name == "mammouth":
        logging.info("Mammouth cluster detected.")
        return MammouthQueue(name=name, walltime=walltime, cores=cores, gpus=gpus, modules=modules)
    elif cluster_name == "guillimin":
        logging.info("Guillimin cluster detected.")
        return GuilliminQueue(name=name, walltime=walltime, cores=cores, gpus=gpus, modules=modules)

    logging.warn("Unknown cluster, returning generic `Queue`.")
    return Queue(name=name, walltime=walltime, cores=cores, gpus=gpus, modules=modules)


class Queue:
    """ Offers functionalities to generate PBS files for a specific queue.

    Parameters
    ----------
    queue : str
        name of the queue on which commands will be excuted
    walltime : str
        maximum time allocated to execute every commands (DD:HH:MM:SS)
    cores : int
        number of cores available per node
    gpus : int
        number of gpus available per node
    modules : list of str
        list of modules to load prior executing commands
    """
    def __init__(self, name, walltime, cores=1, gpus=0, modules=[]):
        if name is None:
            raise ValueError("Queue's name must be specified.")

        if walltime is None:
            raise ValueError("Walltime must be provided.")

        if cores is None:
            raise ValueError("Number of cores must be provided.")

        self.name = name
        self.walltime = walltime
        self.cores = cores
        self.gpus = 0 if gpus is None else gpus
        self.modules = [] if modules is None else modules

    def generate_pbs(self, commands, nb_cores_per_command=1, mem_per_command=0):
        """ Generates PBS objects allowing the execution of every commands on this queue.

        Parameters
        ----------
        commands : list of str
            commands to put in PBS files
        nb_cores_per_command : int
            number of cores needed to execute a single command
        mem_per_command : int
            number of memory (RAM) needed to execute a single command (in bytes)
        """
        cores = self.gpus if self.gpus > 0 else self.cores
        nb_commands_per_node = min(len(commands)*nb_cores_per_command, cores//nb_cores_per_command)

        pbs_files = []
        # Distribute equally the jobs among the PBS files and generate those files
        for i, commands in enumerate(utils.chunks(commands, n=nb_commands_per_node)):
            pbs = PBS(self.name, self.walltime)

            # Set resource: nodes
            ppn = len(commands) * nb_cores_per_command
            resource = "1:ppn={ppn}".format(ppn=ppn)
            if self.gpus > 0:
                resource += ":gpus={gpus}".format(gpus=ppn)

            pbs.add_resources(nodes=resource)

            pbs.add_modules_to_load(*self.modules)
            pbs.add_commands(*commands)

            pbs_files.append(pbs)

        return pbs_files


class MammouthQueue(Queue):
    pass


class GuilliminQueue(Queue):
    def generate_pbs(self, *args, **kwargs):
        pbs_list = Queue.generate_pbs(self, *args, **kwargs)

        if 'HOME_GROUP' not in os.environ:
            logging.warn("Undefined environment variable: $HOME_GROUP.")
            logging.warn("Please, provide your account name if on Guillimin!")

        account_name = os.path.split(os.getenv('HOME_GROUP', ''))[-1]
        for pbs in pbs_list:
            pbs.add_options(A=account_name)

        return pbs_list
