import re
import hashlib
import unicodedata


def chunks(sequence, n):
    """ Yield successive n-sized chunks from sequence. """
    for i in xrange(0, len(sequence), n):
        yield sequence[i:i+n]


def generate_uid(obj):
    """ Create unique identifier from a command. """
    return hashlib.sha256(repr(obj)).hexdigest()


def slugify(value):
    """
    Converts to lowercase, removes non-word characters (alphanumerics and
    underscores) and converts spaces to hyphens. Also strips leading and
    trailing whitespace.

    Reference
    ---------
    https://github.com/django/django/blob/1.7c3/django/utils/text.py#L436
    """
    value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    return str(re.sub('[-\s]+', '_', value))
