import os
import time
import fcntl
import psutil
import logging

from contextlib import contextmanager

# Constants needed for `open_with_dirlock` function.
MAX_ATTEMPTS = 1000  # This would correspond to be blocked for ~15min.
TIME_BETWEEN_ATTEMPTS = 1  # In seconds


def find_mount_point(path='.'):
    """ Finds the mount point used to access `path`. """
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)

    return path


def get_fs(path='.'):
    """ Gets info about the filesystem on which `path` lives. """
    mount = find_mount_point(path)

    for fs in psutil.disk_partitions(True):
        if fs.mountpoint == mount:
            return fs


@contextmanager
def open_with_flock(*args, **kwargs):
    """ Context manager for opening file with an exclusive lock. """
    f = open(*args, **kwargs)
    try:
        fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logging.info("Can't immediately write-lock the file ({0}), waiting ...".format(f.name))
        fcntl.lockf(f, fcntl.LOCK_EX)

    yield f
    fcntl.lockf(f, fcntl.LOCK_UN)
    f.close()


@contextmanager
def open_with_dirlock(*args, **kwargs):
    """ Context manager for opening file with an exclusive lock using. """
    dirname = os.path.dirname(args[0])
    filename = os.path.basename(args[0])
    lockfile = os.path.join(dirname, "." + filename)

    no_attempt = 0
    while no_attempt < MAX_ATTEMPTS:
        try:
            os.mkdir(lockfile)  # Atomic operation
            f = open(*args, **kwargs)
            yield f
            f.close()
            os.rmdir(lockfile)
            break
        except OSError:
            logging.info("Can't immediately write-lock the file ({0}), retrying in {1} sec. ...".format(filename, TIME_BETWEEN_ATTEMPTS))
            time.sleep(TIME_BETWEEN_ATTEMPTS)
            no_attempt += 1


def _fs_support_globalflock(fs):
    if fs.fstype == "lustre":
        return ("flock" in fs.opts) and "localflock" not in fs.opts

    elif fs.fstype == "gpfs":
        return True

    return False  # We don't know.


# Determine if we can rely on the fcntl module for locking files on the cluster.
# Otherwise, fallback on using the directory creation atomicity as a locking mechanism.
fs = get_fs('.')
if _fs_support_globalflock(fs):
    open_with_lock = open_with_flock
else:
    logging.warn("Cluster does not support flock! Falling back to folder lock.")
    open_with_lock = open_with_dirlock
