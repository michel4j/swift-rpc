
from setuptools import setup, find_packages

with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = f.read().splitlines()

with open("README.rst", "r", encoding='utf-8') as fh:
    long_description = fh.read()

def my_version():
    from setuptools_scm.version import get_local_dirty_tag

    def clean_scheme(version):
        return get_local_dirty_tag(version) if version.dirty else ''

    def version_scheme(version):
        print(str(version))
        return str(version.format_with('{tag}.{distance}'))

    return {'local_scheme': clean_scheme, 'version_scheme': version_scheme}


setup(
    name='szrpc',
    use_scm_version=my_version,
    url="https://github.com/michel4j/swift-rpc",
    license='MIT',
    author='Michel Fodje',
    author_email='michel4j@gmail.com',
    description='A simple Python RPC Library using ZeroMQ & MsgPack',
    long_description=long_description,
    long_description_content_type="text/x-rst",
    keywords='rpc networking development',
    packages=find_packages(),
    install_requires=requirements + [
        'importlib-metadata ~= 1.0 ; python_version < "3.8"', "setuptools_scm"
    ],
    classifiers=[
        'Intended Audience :: Developers',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
