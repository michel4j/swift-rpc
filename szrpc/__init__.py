try:
    from importlib.metadata import version, PackageNotFoundError
    __version__ = version("szrpc")
except (ImportError, PackageNotFoundError):
    __version__ = "unknown version"

