import os
import time
import tempfile
import shutil

from subprocess import Popen, PIPE
from nose.tools import assert_equal, assert_true

from smartdispatch.filelock import open_with_lock, open_with_dirlock, open_with_flock
from smartdispatch.filelock import find_mount_point, get_fs


def _test_open_with_lock(lock_func):
    temp_dir = tempfile.mkdtemp()
    filename = os.path.join(temp_dir, "testing.lck")

    python_script = os.path.join(temp_dir, "test_lock.py")

    script = ["import logging",
              "from smartdispatch.filelock import {}".format(lock_func.__name__),
              "logging.root.setLevel(logging.INFO)",
              "with {}('{}', 'r+'): pass".format(lock_func.__name__, filename)]

    open(os.path.join(temp_dir, "test_lock.py"), 'w').write("\n".join(script))

    command = "python " + python_script

    # Lock the commands file before running python command
    with lock_func(filename, 'w'):
        process = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        time.sleep(1)

    stdout, stderr = process.communicate()
    assert_equal(stdout, "")
    assert_true("Traceback" not in stderr, msg="Unexpected error: '{}'".format(stderr))
    assert_true("write-lock" in stderr, msg="Forcing a race condition, try increasing sleeping time above.")

    shutil.rmtree(temp_dir)  # Cleaning up.


def test_open_with_default_lock():
    _test_open_with_lock(open_with_lock)


def test_open_with_dirlock():
    _test_open_with_lock(open_with_dirlock)


def test_open_with_flock():
    _test_open_with_lock(open_with_flock)


def test_find_mount_point():
    assert_equal(find_mount_point('/'), '/')

    for d in os.listdir('/mnt'):
        path = os.path.join('/mnt', d)
        if os.path.ismount(path):
            assert_equal(find_mount_point(path), path)
        else:
            assert_equal(find_mount_point(path), '/')


def test_get_fs():
    fs = get_fs('/')
    assert_true(fs is not None)
