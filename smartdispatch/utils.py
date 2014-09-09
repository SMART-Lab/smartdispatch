import hashlib


def chunks(sequence, n):
    """ Yield successive n-sized chunks from sequence.
    """
    for i in xrange(0, len(sequence), n):
        yield sequence[i:i+n]


def generate_uid_from_command(command):
    """ Create unique identifier from a command. """
    hasher = hashlib.sha1()
    hasher.update(command)
    return hasher.hexdigest()


def generate_name_from_command(command):
    name = ''

    for argvalue in command.split():
        # Use last 30 characters of argvalue and remove '/'
        name += argvalue[-30:].split('/')[-1]
        name += '-'

    return name[:-1]  # Omit last '-'
