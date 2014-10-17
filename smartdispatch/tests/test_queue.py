from nose.tools import assert_equal
from numpy.testing import assert_array_equal

from smartdispatch.queue import Queue
from smartdispatch import get_available_queues
import smartdispatch

import os
import unittest


class TestQueue(unittest.TestCase):
    def setUp(self):
        self.cluster_name = "skynet"
        self.name = "9000@hal"
        self.walltime = "10:00"
        self.cores = 42
        self.gpus = 42
        self.mem_per_node = 32
        self.modules = ["cuda", "python"]

        smartdispatch_dir, _ = os.path.split(smartdispatch.__file__)
        config_dir = os.path.join(smartdispatch_dir, 'config')
        self.known_clusters = [os.path.splitext(config_file)[0] for config_file in os.listdir(config_dir)]

    def test_constructor(self):
        queue = Queue(self.name, self.cluster_name, self.walltime, self.cores, self.gpus, self.mem_per_node, self.modules)
        assert_equal(queue.name, self.name)
        assert_equal(queue.cluster_name, self.cluster_name)
        assert_equal(queue.walltime, self.walltime)
        assert_equal(queue.nb_cores_per_node, self.cores)
        assert_equal(queue.nb_gpus_per_node, self.gpus)
        assert_equal(queue.mem_per_node, self.mem_per_node)
        assert_array_equal(queue.modules, self.modules)

        # Test with missing information but referring to a known queue.
        for cluster_name in self.known_clusters:
            for queue_name, queue_infos in get_available_queues(cluster_name).items():
                queue = Queue(queue_name, cluster_name)
                assert_equal(queue.name, queue_name)
                assert_equal(queue.cluster_name, cluster_name)
                assert_equal(queue.walltime, queue_infos['max_walltime'])
                assert_equal(queue.nb_cores_per_node, queue_infos['cores'])
                assert_equal(queue.nb_gpus_per_node, queue_infos.get('gpus', 0))
                assert_equal(queue.mem_per_node, queue_infos['ram'])
                assert_array_equal(queue.modules, queue_infos.get('modules', []))

                # Make sure it is not overwriting parameters if referring to a known queue.
                queue = Queue(queue_name, cluster_name, self.walltime, self.cores, self.gpus, self.mem_per_node, self.modules)
                assert_equal(queue.name, queue_name)
                assert_equal(queue.cluster_name, cluster_name)
                assert_equal(queue.walltime, self.walltime)
                assert_equal(queue.nb_cores_per_node, self.cores)
                assert_equal(queue.nb_gpus_per_node, self.gpus)
                assert_equal(queue.mem_per_node, self.mem_per_node)
                assert_array_equal(queue.modules, queue_infos.get('modules', []) + self.modules)
