
from setuptools import setup, find_packages
from szrpc import get_version

with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = f.read().splitlines()

with open("README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()


setup(
    name='szrpc',
    version=get_version(),
    url="https://github.com/michel4j/swift-rpc",
    license='MIT',
    author='Michel Fodje',
    author_email='michel4j@gmail.com',
    description='A simple Python RPC Library using ZeroMQ & MsgPack',
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords='rpc networking development',
    packages=find_packages(),
    install_requires=requirements + [
        'importlib-metadata ~= 1.0 ; python_version < "3.8"',
    ],
    classifiers=[
        'Intended Audience :: Developers',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
