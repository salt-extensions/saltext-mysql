"""
Test Salt MySQL state module across various MySQL variants
"""

import logging
import time

import pytest
from pytestshellutils.utils import format_callback_to_string
from saltfactories.utils import random_string
from saltfactories.utils.functional import StateResult

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


class StateSingleWrapper:
    def __init__(self, func, container, ctx):
        self.func = func
        self.container = container
        self.ctx = ctx

    def __call__(self, statefunc, *args, **kwargs):
        if statefunc.startswith("mysql_"):
            kwargs.update(self.container.get_credentials(**kwargs))
            retry = 1
            retries = 3
            ret = None
            while True:
                ret = self.func(statefunc, *list(args), **kwargs.copy())
                if ret.result:
                    break

                mysql_error = _get_mysql_error(self.ctx)
                if mysql_error is None:
                    break

                retry += 1
                if retry > retries:
                    break

                time.sleep(0.5)
                log.debug(
                    "Retrying(%s out of %s) %s because of the following return: %s",
                    retry,
                    retries,
                    format_callback_to_string(statefunc, args, kwargs),
                    ret,
                )
        else:
            # No retries for every other state function
            ret = self.func(statefunc, *args, **kwargs)
        if isinstance(ret, StateResult):
            # Sadly, because we're wrapping, we need to return the raw
            # attribute for a StateResult class to be recreated.
            return ret.raw
        return ret


@pytest.fixture(scope="module")
def mysql(modules, mysql_container, loaders):
    for name in list(modules):
        if name.startswith("mysql."):
            modules._dict[name] = CallWrapper(
                modules._dict[name],
                mysql_container,
                loaders.context,
            )
        if name == "state.single":
            modules._dict[name] = StateSingleWrapper(
                modules._dict[name],
                mysql_container,
                loaders.context,
            )
    return modules.mysql


@pytest.fixture(scope="module")
def mysql_states(mysql, states, mysql_container):
    # Just so we also have the container running
    return states


@pytest.fixture(scope="module")
def mysql_user(mysql_states):
    return mysql_states.mysql_user


@pytest.fixture(scope="module")
def mysql_query(mysql_states):
    return mysql_states.mysql_query


@pytest.fixture(scope="module")
def mysql_grants(mysql_states):
    return mysql_states.mysql_grants


@pytest.fixture(scope="module")
def mysql_database(mysql_states):
    return mysql_states.mysql_database


def test_database_present_absent(mysql_database):
    db_name = random_string("salt_db_", uppercase=False)
    ret = mysql_database.present(name=db_name)
    assert ret.changes
    assert ret.changes == {db_name: "Present"}
    assert ret.comment
    assert ret.comment == f"The database {db_name} has been created"

    ret = mysql_database.absent(name=db_name)
    assert ret.changes
    assert ret.changes == {db_name: "Absent"}
    assert ret.comment
    assert ret.comment == f"Database {db_name} has been removed"


def test_grants_present_absent(mysql, mysql_grants, mysql_db, mysql_account):

    ret = mysql_grants.present(
        name="add_salt_grants",
        grant="select,insert,update",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret.changes
    assert ret.changes == {"add_salt_grants": "Present"}
    assert ret.comment
    assert (
        ret.comment
        == f"Grant select,insert,update on {mysql_db}.* to {mysql_account}@localhost has been added"
    )

    ret = mysql_grants.absent(
        name="delete_salt_grants",
        grant="select,insert,update",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )
    assert ret.changes
    assert ret.changes == {"delete_salt_grants": "Absent"}
    assert ret.comment
    assert (
        ret.comment
        == f"Grant select,insert,update on {mysql_db}.* for {mysql_account}@localhost has been revoked"
    )


@pytest.mark.parametrize("mysql_db", ("saltdb_",), indirect=True)
def test_grants_present_absent_underscore_no_escape(mysql, mysql_grants, mysql_db, mysql_account):
    """
    The absent state must pass escape through to mysql.grant_revoke,
    otherwise grants added with escape=False on databases containing
    wildcard characters cannot be revoked (issues #18, #21).
    """
    # This test relies on the database name containing a wildcard character
    assert "_" in mysql_db

    ret = mysql_grants.present(
        name="add_underscore_db_grants",
        grant="select",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
        escape=False,
    )
    assert ret.changes
    assert ret.changes == {"add_underscore_db_grants": "Present"}
    assert ret.comment
    assert (
        ret.comment == f"Grant select on {mysql_db}.* to {mysql_account}@localhost has been added"
    )

    ret = mysql_grants.absent(
        name="delete_underscore_db_grants",
        grant="select",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
        escape=False,
    )
    assert ret.changes
    assert ret.changes == {"delete_underscore_db_grants": "Absent"}
    assert ret.comment
    assert (
        ret.comment
        == f"Grant select on {mysql_db}.* for {mysql_account}@localhost has been revoked"
    )


def test_grants_present_absent_all_privileges(mysql, mysql_grants, mysql_db, mysql_account):

    ret = mysql_grants.present(
        name="add_salt_grants",
        grant="all privileges",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )

    assert ret.changes
    assert ret.changes == {"add_salt_grants": "Present"}

    assert ret.comment
    assert (
        ret.comment
        == f"Grant all privileges on {mysql_db}.* to {mysql_account}@localhost has been added"
    )

    ret = mysql_grants.absent(
        name="delete_salt_grants",
        grant="all privileges",
        database=f"{mysql_db}.*",
        user=mysql_account,
        host="localhost",
    )

    assert ret.changes
    assert ret.changes == {"delete_salt_grants": "Absent"}

    assert ret.comment
    assert (
        ret.comment
        == f"Grant all privileges on {mysql_db}.* for {mysql_account}@localhost has been revoked"
    )


def test_user_present_absent(mysql_user):
    user_name = random_string("salt_user_", uppercase=False)

    ret = mysql_user.present(
        name=user_name,
        host="localhost",
        password="password",
    )
    assert ret.changes
    assert ret.changes == {user_name: "Present"}
    assert ret.comment
    assert ret.comment == f"The user {user_name}@localhost has been added"

    ret = mysql_user.absent(
        name=user_name,
        host="localhost",
    )
    assert ret.changes
    assert ret.changes == {user_name: "Absent"}
    assert ret.comment
    assert ret.comment == f"User {user_name}@localhost has been removed"


def test_user_present_absent_passwordless(mysql_user):
    user_name = random_string("salt_user_", uppercase=False)

    ret = mysql_user.present(
        name=user_name,
        host="localhost",
        allow_passwordless=True,
    )
    assert ret.changes
    assert ret.changes == {user_name: "Present"}
    assert ret.comment
    assert ret.comment == f"The user {user_name}@localhost has been added with passwordless login"

    ret = mysql_user.absent(
        name=user_name,
        host="localhost",
    )
    assert ret.changes
    assert ret.changes == {user_name: "Absent"}
    assert ret.comment
    assert ret.comment == f"User {user_name}@localhost has been removed"


def test_user_present_absent_unixsocket(mysql, mysql_user, mysql_container):

    # The auth_socket plugin on MariaDB is unavailable
    # on versions 10.1 - 10.3
    if "mariadb" in mysql_container.mysql_name:
        if mysql_container.mysql_version in ("10.1", "10.2", "10.3"):
            pytest.skip(
                "The auth_socket plugin is unavaiable "
                f"for the {mysql_container.mysql_name}:{mysql_container.mysql_version} docker image."
            )

    user_name = random_string("salt_user_", uppercase=False)

    # enable the auth_socket plugin on MySQL
    # already enabled on MariaDB > 10.3
    try:
        if "mariadb" not in mysql_container.mysql_name:
            ret = mysql.plugin_add("auth_socket")
            assert ret

        ret = mysql_user.present(
            name=user_name,
            host="localhost",
            unix_socket=True,
            allow_passwordless=False,
        )
        assert ret.changes
        assert ret.changes == {user_name: "Present"}
        assert ret.comment
        assert ret.comment == f"The user {user_name}@localhost has been added using unix_socket"

        ret = mysql_user.absent(
            name=user_name,
            host="localhost",
        )
        assert ret.changes
        assert ret.changes == {user_name: "Absent"}
        assert ret.comment
        assert ret.comment == f"User {user_name}@localhost has been removed"
    finally:
        if "mariadb" not in mysql_container.mysql_name:
            ret = mysql.plugin_remove("auth_socket")
            assert ret


def test_grants_present_absent_state_file(modules, mysql, mysql_grants, mysql_combo, state_tree):
    name = random_string("sbclient_", uppercase=False)

    content = f"""
    {name}:
      mysql_user.present:
        - host: localhost
        - password: sbclient
        - connection_user: {mysql_combo.mysql_root_user}
        - connection_pass: {mysql_combo.mysql_root_passwd}
        - connection_db: mysql
        - connection_port: {mysql_combo.mysql_port}
      mysql_database.present:
        - connection_user: {mysql_combo.mysql_root_user}
        - connection_pass: {mysql_combo.mysql_root_passwd}
        - connection_db: mysql
        - connection_port: {mysql_combo.mysql_port}
      mysql_grants.present:
        - grant: ALL PRIVILEGES
        - user: {name}
        - database: {name}.*
        - host: localhost
        - connection_user: {mysql_combo.mysql_root_user}
        - connection_pass: {mysql_combo.mysql_root_passwd}
        - connection_db: mysql
        - connection_port: {mysql_combo.mysql_port}
    """

    try:
        with pytest.helpers.temp_file("manage_mysql.sls", content, state_tree):
            ret = modules.state.apply("manage_mysql")

            # Check user creation
            state = f"mysql_user_|-{name}_|-{name}_|-present"
            assert state in ret
            assert "changes" in ret[state]
            assert ret[state].changes == {name: "Present"}

            # Check database creation
            state = f"mysql_database_|-{name}_|-{name}_|-present"
            assert state in ret
            assert "changes" in ret[state]
            assert ret[state].changes == {name: "Present"}

            # Check grant creation
            state = f"mysql_grants_|-{name}_|-{name}_|-present"
            assert state in ret
            assert "comment" in ret[state]
            assert (
                ret[state].comment
                == f"Grant ALL PRIVILEGES on {name}.* to {name}@localhost has been added"
            )

        ret = mysql_grants.absent(
            name="delete_sbclient_grants",
            grant="all privileges",
            database=f"{name}.*",
            user=name,
            host="localhost",
        )
        assert ret.changes
        assert ret.changes == {"delete_sbclient_grants": "Absent"}

        assert ret.comment
        assert (
            ret.comment == f"Grant all privileges on {name}.* for {name}@localhost has been revoked"
        )

    finally:
        # Remove the user
        ret = mysql.user_remove(name, host="localhost")
        assert ret

        # Remove the database
        ret = mysql.db_remove(name)
        assert ret
