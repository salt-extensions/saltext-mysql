import os

import pytest
import salt.config


@pytest.fixture
def minion_opts(tmp_path):  # pragma: no cover
    """
    Default minion configuration with relative temporary paths to not
    require root permissions.
    """
    root_dir = tmp_path / "minion"
    opts = salt.config.DEFAULT_MINION_OPTS.copy()
    opts["__role"] = "minion"
    opts["root_dir"] = str(root_dir)
    for name in ("cachedir", "pki_dir", "sock_dir", "conf_dir"):
        dirpath = root_dir / name
        dirpath.mkdir(parents=True)
        opts[name] = str(dirpath)
    opts["log_file"] = "logs/minion.log"
    opts["conf_file"] = os.path.join(opts["conf_dir"], "minion")
    return opts


@pytest.fixture
def master_opts(tmp_path):  # pragma: no cover
    """
    Default master configuration with relative temporary paths to not
    require root permissions.
    """
    root_dir = tmp_path / "master"
    opts = salt.config.master_config(None)
    opts["__role"] = "master"
    opts["root_dir"] = str(root_dir)
    for name in ("cachedir", "pki_dir", "sock_dir", "conf_dir"):
        dirpath = root_dir / name
        dirpath.mkdir(parents=True)
        opts[name] = str(dirpath)
    opts["log_file"] = "logs/master.log"
    opts["conf_file"] = os.path.join(opts["conf_dir"], "master")
    return opts


@pytest.fixture
def syndic_opts(tmp_path):  # pragma: no cover
    """
    Default master configuration with relative temporary paths to not
    require root permissions.
    """
    root_dir = tmp_path / "syndic"
    opts = salt.config.DEFAULT_MINION_OPTS.copy()
    opts["syndic_master"] = "127.0.0.1"
    opts["__role"] = "minion"
    opts["root_dir"] = str(root_dir)
    for name in ("cachedir", "pki_dir", "sock_dir", "conf_dir"):
        dirpath = root_dir / name
        dirpath.mkdir(parents=True)
        opts[name] = str(dirpath)
    opts["log_file"] = "logs/syndic.log"
    opts["conf_file"] = os.path.join(opts["conf_dir"], "syndic")
    return opts
