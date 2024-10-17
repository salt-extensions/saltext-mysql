import logging

import pytest
import salt.cache
import salt.loader

from tests.functional.cache.helpers import run_common_cache_tests
from tests.support.mysql import *  # pylint: disable=wildcard-import,unused-wildcard-import

docker = pytest.importorskip("docker")

log = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.skip_if_binaries_missing("dockerd"),
]


@pytest.fixture(scope="module")
def mysql_combo(create_mysql_combo):  # pylint: disable=function-redefined
    create_mysql_combo.mysql_database = "salt_cache"
    return create_mysql_combo


@pytest.fixture
def cache(minion_opts, mysql_container):
    opts = minion_opts.copy()
    opts["cache"] = "mysql"
    opts["mysql_cache.host"] = "127.0.0.1"
    opts["mysql_cache.port"] = mysql_container.mysql_port
    opts["mysql_cache.user"] = mysql_container.mysql_user
    opts["mysql_cache.password"] = mysql_container.mysql_passwd
    opts["mysql_cache.database"] = mysql_container.mysql_database
    opts["mysql_cache.table_name"] = "cache"
    cache = salt.cache.factory(opts)
    return cache


def test_caching(subtests, cache):
    run_common_cache_tests(subtests, cache)
