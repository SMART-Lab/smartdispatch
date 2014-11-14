import re
import fcntl
import logging
import hashlib
import unicodedata
import json

from subprocess import Popen, PIPE
from contextlib import contextmanager


def chunks(sequence, n):
    """ Yield successive n-sized chunks from sequence. """
    for i in xrange(0, len(sequence), n):
        yield sequence[i:i + n]


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


def escape(text, escaping_character="\\"):
    """ Escape the escaped character using its hex representation """
    def hexify(match):
        return "\\x{0}".format(match.group()[-1].encode("hex"))

    return re.sub(r"\\.", hexify, text)


def hex2str(text):
    """ Convert hex representation to the character it represents """
    if len(text) == 0:
        return ''

    def unhexify(match):
        return match.group()[2:].decode("hex")

    return re.sub(r"\\x..", unhexify, text)

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


def save_dict_to_json_file(path, dictionary):
    with open(path, "w") as json_file:
        json_file.write(json.dumps(dictionary, indent=4, separators=(',', ': ')))


def load_dict_from_json_file(path):
    with open(path, "r") as json_file:
        return json.loads(json_file.read())


def detect_cluster():
    # Get server status
    try:
        output = Popen(["qstat", "-B"], stdout=PIPE).communicate()[0]
    except OSError:
        # If qstat is not available we assume that the cluster is unknown.
        return None
    # Get server name from status
    server_name = output.split('\n')[2].split(' ')[0]
    # Cleanup the name and return it
    cluster_name = None
    if server_name.split('.')[-1] == 'm':
        cluster_name = "mammouth"
    elif server_name.split('.')[-1] == 'guil':
        cluster_name = "guillimin"
    elif server_name.split('.')[-1] == 'helios':
        cluster_name = "helios"
    return cluster_name
