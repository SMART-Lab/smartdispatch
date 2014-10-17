from smartdispatch import get_available_queues


class Queue(object):
    def __init__(self, name, cluster_name, walltime=None, nb_cores_per_node=None, nb_gpus_per_node=None, mem_per_node=None, modules=None):
        self.name = name
        self.cluster_name = cluster_name

        self.walltime = walltime
        self.nb_cores_per_node = nb_cores_per_node
        self.nb_gpus_per_node = nb_gpus_per_node
        self.mem_per_node = mem_per_node
        self.modules = modules if modules is not None else []

        available_queues = get_available_queues(self.cluster_name)
        if self.name in available_queues:
            queue_infos = available_queues[self.name]

            if self.walltime is None:
                self.walltime = queue_infos['max_walltime']
            if self.nb_cores_per_node is None:
                self.nb_cores_per_node = queue_infos['cores']
            if self.nb_gpus_per_node is None:
                self.nb_gpus_per_node = queue_infos.get('gpus', 0)
            if self.mem_per_node is None:
                self.mem_per_node = queue_infos['ram']

            # Add default modules to load for this queue if any.
            self.modules = queue_infos.get('modules', []) + self.modules

        # Make sure Queue instance is in a valid state.
        if self.walltime is None:
            raise ValueError("Walltime must be provided on this queue!")

        if self.nb_cores_per_node is None or self.nb_cores_per_node <= 0:
            raise ValueError("Queues must have at least one core!")

        if self.nb_gpus_per_node is None:
            self.nb_gpus_per_node = 0  # Means, there are no gpus on those queue nodes

        if self.mem_per_node is None or self.mem_per_node <= 0:
            raise ValueError("Queues must have at least some memory!")
