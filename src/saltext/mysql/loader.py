"""
Define the required entry-points functions in order for Salt to know
what and from where it should load this extension's loaders
"""
from . import PACKAGE_ROOT  # pylint: disable=unused-import


def get_auth_dirs():
    """
    Return a list of paths from where salt should load auth modules
    """
    return [str(PACKAGE_ROOT / "auths")]


def get_cache_dirs():
    """
    Return a list of paths from where salt should load cache modules
    """
    return [str(PACKAGE_ROOT / "caches")]


def get_module_dirs():
    """
    Return a list of paths from where salt should load module modules
    """
    return [str(PACKAGE_ROOT / "modules")]


def get_pillar_dirs():
    """
    Return a list of paths from where salt should load pillar modules
    """
    return [str(PACKAGE_ROOT / "pillars")]


def get_returner_dirs():
    """
    Return a list of paths from where salt should load returner modules
    """
    return [str(PACKAGE_ROOT / "returners")]


def get_state_dirs():
    """
    Return a list of paths from where salt should load state modules
    """
    return [str(PACKAGE_ROOT / "states")]


