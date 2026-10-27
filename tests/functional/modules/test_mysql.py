"""
Test Salt MySQL module across various MySQL variants
"""

import logging
import time

import pytest
from pytestshellutils.utils import format_callback_to_string
from salt.utils.versions import version_cmp
from saltfactories.utils import random_string

from saltext.mysql.modules import mysql as mysqlmod
from tests.support.mysql import *  # pylint: disable=wildcard-import,unused-wildcard-import

log = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.skip_if_binaries_missing("dockerd"),
    pytest.mark.skipif(mysqlmod.MySQLdb is None, reason="No python mysql client installed."),
]


def _get_mysql_error(context):
    return context.pop("mysql.error", None)


class CallWrapper:
    def __init__(self, func, container, ctx):
        self.func = func
        self.container = container
        self.ctx = ctx

    def __call__(self, *args, **kwargs):
        kwargs.update(self.container.get_credentials(**kwargs))
        retry = 1
        retries = 3
        ret = None
        while True:
            ret = self.func(*list(args), **kwargs.copy())
            mysql_error = _get_mysql_error(self.ctx)
            if mysql_error is None:
                break
            retry += 1

            if retry > retries:
                break

            time.sleep(0.5)
            log.debug(
                "Retrying(%s out of %s) %s because of the following error: %s",
                retry,
                retries,
                format_callback_to_string(self.func, args, kwargs),
                mysql_error,
            )
        return ret


@pytest.fixture(scope="module")
def mysql(modules, mysql_container, loaders):
    for name in list(modules):
        if not name.startswith("mysql."):
            continue
        modules._dict[name] = CallWrapper(
            modules._dict[name],
            mysql_container,
            loaders.context,
        )
    return modules.mysql


def test_query(mysql):
    ret = mysql.query("mysql", "SELECT 1")
    assert ret
    assert ret["results"] == (("1",),)


def test_version(mysql, mysql_container):
    ret = mysql.version()
    assert ret
    assert mysql_container.mysql_version in ret


def test_status(mysql):
    ret = mysql.status()
    assert ret


def test_db_list(mysql):
    ret = mysql.db_list()
    assert ret
    assert "mysql" in ret


def test_db_create_alter_remove(mysql):
    db_name = random_string("salt_db_", uppercase=False)
    ret = mysql.db_create(db_name)
    assert ret

    try:
        ret = mysql.alter_db(
            name=db_name,
            character_set="latin1",
            collate="latin1_general_ci",
        )
        assert ret
    finally:
        ret = mysql.db_remove(name=db_name)
        assert ret


def test_user_list(mysql, mysql_combo):
    ret = mysql.user_list()
    assert ret
    assert {
        "User": mysql_combo.mysql_root_user,
        "Host": mysql_combo.mysql_host,
    } in ret


def test_user_exists(mysql, mysql_combo):
    ret = mysql.user_exists(
        mysql_combo.mysql_root_user,
        host=mysql_combo.mysql_host,
        password=mysql_combo.mysql_passwd,
    )
    assert ret

    ret = mysql.user_exists(
        "george",
        "hostname",
        "badpassword",
    )
    assert not ret


def test_user_info(mysql, mysql_combo):
    ret = mysql.user_info(mysql_combo.mysql_root_user, host=mysql_combo.mysql_host)
    assert ret

    # Check that a subset of the information
    # is available in the returned user information.
    expected = {
        "Host": mysql_combo.mysql_host,
        "User": mysql_combo.mysql_root_user,
        "Select_priv": "Y",
        "Insert_priv": "Y",
        "Update_priv": "Y",
        "Delete_priv": "Y",
        "Create_priv": "Y",
        "Drop_priv": "Y",
        "Reload_priv": "Y",
        "Shutdown_priv": "Y",
        "Process_priv": "Y",
        "File_priv": "Y",
        "Grant_priv": "Y",
        "References_priv": "Y",
        "Index_priv": "Y",
        "Alter_priv": "Y",
        "Show_db_priv": "Y",
        "Super_priv": "Y",
        "Create_tmp_table_priv": "Y",
        "Lock_tables_priv": "Y",
        "Execute_priv": "Y",
        "Repl_slave_priv": "Y",
        "Repl_client_priv": "Y",
        "Create_view_priv": "Y",
        "Show_view_priv": "Y",
        "Create_routine_priv": "Y",
        "Alter_routine_priv": "Y",
        "Create_user_priv": "Y",
        "Event_priv": "Y",
        "Trigger_priv": "Y",
        "Create_tablespace_priv": "Y",
    }
    data = ret.copy()
    for key in list(data):
        if key not in expected:
            data.pop(key)
    assert data == expected


def test_user_create_chpass_delete(mysql):
    user_name = random_string("salt_user_", uppercase=False)
    ret = mysql.user_create(
        user_name,
        host="localhost",
        password="badpassword",
    )
    assert ret

    try:
        ret = mysql.user_chpass(
            user_name,
            host="localhost",
            password="different_password",
        )
        assert ret
    finally:
        ret = mysql.user_remove(user_name, host="localhost")
        assert ret


def test_user_grants(mysql, mysql_combo):
    ret = mysql.user_grants(mysql_combo.mysql_root_user, host=mysql_combo.mysql_host)
    assert ret


def test_grant_add_revoke(mysql, mysql_db, mysql_account):
    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="ALL PRIVILEGES",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="ALL PRIVILEGES",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the grant
    ret = mysql.grant_revoke(
        grant="ALL PRIVILEGES",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="ALL PRIVILEGES",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret

    # Grant privileges to user globally
    ret = mysql.grant_add(
        grant="ALL PRIVILEGES",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the global exists
    ret = mysql.grant_exists(
        grant="ALL PRIVILEGES",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="ALL PRIVILEGES",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="ALL PRIVILEGES",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


@pytest.mark.parametrize("mysql_db", ("saltdb_",), indirect=True)
def test_grant_add_revoke_underscore(mysql, mysql_db, mysql_account):
    """
    Grants on databases with wildcard characters in their name must be
    revocable both with the default escaping and with escape=False (issue #18).
    """
    # This test relies on the database name containing a wildcard character
    assert "_" in mysql_db

    # Grant privileges with the default escaping (grants on the literal name)
    ret = mysql.grant_add(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the grant
    ret = mysql.grant_revoke(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret

    # Grant privileges without escaping the database name wildcards
    ret = mysql.grant_add(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
        escape=False,
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
        escape=False,
    )
    assert ret

    # Revoking must honor escape=False as well, otherwise the escaped
    # database name does not match the stored grant
    ret = mysql.grant_revoke(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
        escape=False,
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="SELECT",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
        escape=False,
    )
    assert not ret


def test_grant_replication_replica_add_revoke(mysql, mysql_container, mysql_account):
    # The REPLICATION REPLICA grant is only available for mariadb
    if "mariadb" not in mysql_container.mysql_name:
        pytest.skip(
            "The REPLICATION REPLICA grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # The REPLICATION REPLICA grant was added in mariadb 10.5.1
    if version_cmp(mysql_container.mysql_version, "10.5.1") < 0:
        pytest.skip(
            "The REPLICATION REPLICA grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="REPLICATION REPLICA",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="REPLICATION REPLICA",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="REPLICATION REPLICA",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="REPLICATION REPLICA",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


def test_grant_replication_slave_add_revoke(mysql, mysql_container, mysql_account):
    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="REPLICATION SLAVE",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="REPLICATION SLAVE",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="REPLICATION SLAVE",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="REPLICATION SLAVE",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


def test_grant_replication_client_add_revoke(mysql, mysql_container, mysql_account):
    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="REPLICATION CLIENT",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="REPLICATION CLIENT",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="REPLICATION CLIENT",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="REPLICATION CLIENT",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


def test_grant_binlog_monitor_add_revoke(mysql, mysql_container, mysql_db, mysql_account):
    # The BINLOG MONITOR grant is only available for mariadb
    if "mariadb" not in mysql_container.mysql_name:
        pytest.skip(
            "The BINLOG MONITOR grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # The BINLOG MONITOR grant was added in mariadb 10.5.2
    if version_cmp(mysql_container.mysql_version, "10.5.2") < 0:
        pytest.skip(
            "The BINLOG_MONITOR grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="BINLOG MONITOR",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="BINLOG MONITOR",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="BINLOG MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="BINLOG MONITOR",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


def test_grant_replica_monitor_add_revoke(mysql, mysql_container, mysql_account):
    # The REPLICA MONITOR grant is only available for mariadb
    if "mariadb" not in mysql_container.mysql_name:
        pytest.skip(
            "The REPLICA MONITOR grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # The REPLICA MONITOR grant was added in mariadb 10.5.9
    if version_cmp(mysql_container.mysql_version, "10.5.9") < 0:
        pytest.skip(
            "The REPLICA MONITOR grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="REPLICA MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="REPLICA MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="REPLICA MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="REPLICA MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


def test_grant_slave_monitor_add_revoke(mysql, mysql_container, mysql_db, mysql_account):
    # The SLAVE MONITOR grant is only available for mariadb
    if "mariadb" not in mysql_container.mysql_name:
        pytest.skip(
            "The SLAVE MONITOR grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # The SLAVE MONITOR grant was added in mariadb 10.5.9
    if version_cmp(mysql_container.mysql_version, "10.5.9") < 0:
        pytest.skip(
            "The SLAVE MONITOR grant is unavailable "
            f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
        )

    # Grant privileges to user to specific table
    ret = mysql.grant_add(
        grant="SLAVE MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant exists
    ret = mysql.grant_exists(
        grant="SLAVE MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Revoke the global grant
    ret = mysql.grant_revoke(
        grant="SLAVE MONITOR",
        database="*.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret

    # Check the grant does not exist
    ret = mysql.grant_exists(
        grant="SLAVE MONITOR",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert not ret


def test_plugin_add_status_remove(mysql, mysql_combo):

    if "mariadb" in mysql_combo.mysql_name:
        plugin = "simple_password_check"
    else:
        plugin = "auth_socket"

    ret = mysql.plugin_status(plugin, host=mysql_combo.mysql_host)
    assert not ret

    ret = mysql.plugin_add(plugin)
    assert ret

    ret = mysql.plugin_status(plugin, host=mysql_combo.mysql_host)
    assert ret
    assert ret == "ACTIVE"

    ret = mysql.plugin_remove(plugin)
    assert ret

    ret = mysql.plugin_status(plugin, host=mysql_combo.mysql_host)
    assert not ret


def test_plugin_list(mysql, mysql_container):
    if "mariadb" in mysql_container.mysql_name:
        plugin = "simple_password_check"
    else:
        plugin = "auth_socket"

    ret = mysql.plugins_list()
    assert {"name": plugin, "status": "ACTIVE"} not in ret
    assert ret

    ret = mysql.plugin_add(plugin)
    assert ret

    ret = mysql.plugins_list()
    assert ret
    assert {"name": plugin, "status": "ACTIVE"} in ret

    ret = mysql.plugin_remove(plugin)
    assert ret


def test_grant_add_revoke_password_hash(mysql, mysql_db):
    # Create a user
    user_name = random_string("salt_user_", uppercase=False)
    ret = mysql.user_create(
        user_name,
        host="%",
        password_hash="*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
    )
    assert ret

    try:
        # Grant privileges to user to specific table
        ret = mysql.grant_add(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert ret

        # Check the grant exists
        ret = mysql.grant_exists(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert ret

        # Check the grant exists via a query
        ret = mysql.query(
            database=mysql_db,
            query="SELECT 1",
            connection_user=user_name,
            connection_pass="password",
            connection_db=mysql_db,
        )
        assert ret

        # Revoke the grant
        ret = mysql.grant_revoke(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert ret

        # Check the grant does not exist
        ret = mysql.grant_exists(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert not ret

    finally:
        # Remove the user
        ret = mysql.user_remove(user_name, host="%")
        assert ret


def test_create_alter_password_hash(mysql, mysql_db):
    # Create a user
    user_name = random_string("salt_user_", uppercase=False)
    ret = mysql.user_create(
        user_name,
        host="%",
        password_hash="*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
    )
    assert ret

    try:
        # Grant privileges to user to specific table
        ret = mysql.grant_add(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert ret

        # Check the grant exists
        ret = mysql.grant_exists(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert ret

        # Check we can query as the new user
        ret = mysql.query(
            database=mysql_db,
            query="SELECT 1",
            connection_user=user_name,
            connection_pass="password",
            connection_db=mysql_db,
        )
        assert ret

        # Change the user password
        ret = mysql.user_chpass(
            user_name,
            host="%",
            password_hash="*F4A5147613F01DEC0C5226BF24CD1D5762E6AAF2",
        )
        assert ret

        # Check we can query with the new password
        ret = mysql.query(
            database=mysql_db,
            query="SELECT 1",
            connection_user=user_name,
            connection_pass="badpassword",
            connection_db=mysql_db,
        )
        assert ret

        # Revoke the grant
        ret = mysql.grant_revoke(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert ret

        # Check the grant does not exist
        ret = mysql.grant_exists(
            grant="ALL PRIVILEGES",
            database=f"{mysql_db}.*",
            user=user_name,
            host="%",
        )
        assert not ret

    finally:
        # Remove the user
        ret = mysql.user_remove(user_name, host="%")
        assert ret
