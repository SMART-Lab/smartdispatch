import re
import fcntl
import logging
import hashlib
import unicodedata

from contextlib import contextmanager


def chunks(sequence, n):
    """ Yield successive n-sized chunks from sequence. """
    for i in xrange(0, len(sequence), n):
        yield sequence[i:i+n]


def generate_uid_from_string(value):
    """ Create unique identifier from a string. """
    return hashlib.sha256(value).hexdigest()


def slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to underscores. Also strips leading and
    trailing whitespace.

    Reference
    ---------
    https://github.com/django/django/blob/1.7c3/django/utils/text.py#L436
    """
    value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return str(re.sub('[-\s]+', '_', value))


@contextmanager
def open_with_lock(*args, **kwargs):
    """ Context manager for opening file with an exclusive lock. """
    f = open(*args, **kwargs)
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logging.info("Can't immediately write-lock the file ({0}), blocking ...".format(f.name))
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    yield f
    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    f.close()
