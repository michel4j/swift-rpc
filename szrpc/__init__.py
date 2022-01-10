import os
import re
from subprocess import CalledProcessError, check_output


PACKAGE_DIR = os.path.dirname(os.path.dirname(__file__))


def get_version(prefix='v', package=PACKAGE_DIR, name=None):

    # Return the version if it has been injected into the file by git-archive
    tag_re = re.compile(rf'\btag: {prefix}([0-9][^,]*)\b')
    version = tag_re.search('$Format:%D$')
    name = __name__.split('.')[0] if not name else name

    if version:
        return version.group(1)

    package_dir = package

    if os.path.isdir(os.path.join(package_dir, '.git')):
        # Get the version using "git describe".
        version_cmd = 'git describe --tags --abbrev=0'
        release_cmd = 'git rev-list HEAD ^$(git describe --abbrev=0) | wc -l'
        try:
            version = check_output(version_cmd, shell=True).decode().strip()
            release = check_output(release_cmd, shell=True).decode().strip()
            return f'{version}.{release}'.strip(prefix)
        except CalledProcessError:
            version = '0.0'
            release = 'dev'
            return f'{version}.{release}'.strip(prefix)
    else:
        try:
            from importlib import metadata
        except ImportError:
            # Running on pre-3.8 Python; use importlib-metadata package
            import importlib_metadata as metadata

        version = metadata.version(name)

    return version
