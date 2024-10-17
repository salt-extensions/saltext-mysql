"""
Minion data cache plugin for MySQL database.

.. versionadded:: 2018.3.0

It is up to the system administrator to set up and configure the MySQL
infrastructure. All is needed for this plugin is a working MySQL server.

.. warning::

    The mysql_cache.database and mysql_cache.table_name will be directly added into certain
    queries. Salt treats these as trusted input.

To enable this cache plugin, the master will need the python client for
MySQL installed. This can be easily installed with pip:

.. code-block:: bash

    pip install pymysql

Optionally, depending on the MySQL agent configuration, the following values
could be set in the master config. These are the defaults:

.. code-block:: yaml

    mysql_cache.host: 127.0.0.1
    mysql_cache.port: 3306
    mysql_cache.user: None
    mysql_cache.password: None
    mysql_cache.database: salt_cache
    mysql_cache.table_name: cache
    # This may be enabled to create a fresh connection on every call
    mysql_cache.fresh_connection: false

Use the following mysql database schema:

.. code-block:: sql

    CREATE DATABASE salt_cache;
    USE salt_cache;

    CREATE TABLE IF NOT EXISTS cache (
          bank VARCHAR(255),
          cache_key VARCHAR(255),
          data MEDIUMBLOB,
          last_update TIMESTAMP NOT NULL
                      DEFAULT CURRENT_TIMESTAMP
                      ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY(bank, cache_key)
        );

To use the mysql as a minion data cache backend, set the master ``cache`` config
value to ``mysql``:

.. code-block:: yaml

    cache: mysql

.. _`MySQL documentation`: https://github.com/coreos/mysql
"""

import copy
import logging
import time

import salt.payload
import salt.utils.stringutils
from salt.exceptions import SaltCacheError

try:
    # Trying to import MySQLdb
    import MySQLdb
    import MySQLdb.converters
    import MySQLdb.cursors
    from MySQLdb.connections import OperationalError

    # Define the interface error as a subclass of exception
    # It will never be thrown/used, it is defined to support the pymysql error below
    class InterfaceError(Exception):
        pass

except ImportError:
    try:
        # MySQLdb import failed, try to import PyMySQL
        import pymysql
        from pymysql.err import InterfaceError

        pymysql.install_as_MySQLdb()
        import MySQLdb
        import MySQLdb.converters
        import MySQLdb.cursors
        from MySQLdb.err import OperationalError
    except ImportError:
        MySQLdb = None


_RECONNECT_INTERVAL_SEC = 0.050

log = logging.getLogger(__name__)

# Module properties

__virtualname__ = "mysql"
__func_alias__ = {"list_": "list"}


def __virtual__():
    """
    Confirm that a python mysql client is installed.
    """
    return bool(MySQLdb), "No python mysql client installed." if MySQLdb is None else ""


def force_reconnect():
    """
    Force a reconnection to the MySQL database, by removing the client from
    Salt's __context__.
    """
    __context__.pop("mysql_client", None)


def run_query(conn, query, args=None, retries=3):
    """
    Get a cursor and run a query. Reconnect up to ``retries`` times if
    needed.
    Returns: cursor, affected rows counter
    Raises: SaltCacheError, AttributeError, OperationalError, InterfaceError
    """
    if __context__.get("mysql_fresh_connection"):
        # Create a new connection if configured
        conn = MySQLdb.connect(**__context__["mysql_kwargs"])
        __context__["mysql_client"] = conn
    if conn is None:
        conn = __context__.get("mysql_client")
    try:
        cur = conn.cursor()

        if not args:
            log.debug("Doing query: %s", query)
            out = cur.execute(query)
        else:
            log.debug("Doing query: %s args: %s ", query, repr(args))
            out = cur.execute(query, args)

        return cur, out
    except (AttributeError, OperationalError, InterfaceError) as e:
        if retries == 0:
            raise
        # reconnect creating new client
        time.sleep(_RECONNECT_INTERVAL_SEC)
        if conn is None:
            log.debug("mysql_cache: creating db connection")
        else:
            log.info("mysql_cache: recreating db connection due to: %r", e)
        __context__["mysql_client"] = MySQLdb.connect(**__context__["mysql_kwargs"])
        return run_query(
            conn=__context__.get("mysql_client"),
            query=query,
            args=args,
            retries=(retries - 1),
        )
    except Exception as e:  # pylint: disable=broad-except
        if len(query) > 150:
            query = query[:150] + "<...>"
        raise SaltCacheError(
            "Error running {}{}: {}".format(query, f"- args: {args}" if args else "", e)
        ) from e


def _init_client():
    """Initialize connection and create table if needed"""
    if __context__.get("mysql_client") is not None:
        return

    opts = copy.deepcopy(__opts__)
    mysql_kwargs = {
        "autocommit": True,
        "host": opts.pop("mysql_cache.host", "127.0.0.1"),
        "user": opts.pop("mysql_cache.user", None),
        "passwd": opts.pop("mysql_cache.password", None),
        "db": opts.pop("mysql_cache.database", "salt_cache"),
        "port": opts.pop("mysql_cache.port", 3306),
        "unix_socket": opts.pop("mysql_cache.unix_socket", None),
        "connect_timeout": opts.pop("mysql_cache.connect_timeout", None),
    }
    mysql_kwargs["autocommit"] = True

    __context__["mysql_table_name"] = opts.pop("mysql_cache.table_name", "cache")
    __context__["mysql_fresh_connection"] = opts.pop("mysql_cache.fresh_connection", False)

    # Gather up any additional MySQL configuration options
    for k in opts:
        if k.startswith("mysql_cache."):
            _key = k.split(".")[1]
            mysql_kwargs[_key] = opts.get(k)

    # TODO: handle SSL connection parameters

    for k, v in copy.deepcopy(mysql_kwargs).items():
        if v is None:
            mysql_kwargs.pop(k)
    kwargs_copy = mysql_kwargs.copy()
    kwargs_copy["passwd"] = "<hidden>"
    log.info("Cache mysql: Setting up client with params: %r", kwargs_copy)
    __context__["mysql_kwargs"] = mysql_kwargs


def store(bank, key, data):
    """
    Store a key value.
    """
    _init_client()
    data = salt.payload.dumps(data)
    query = "REPLACE INTO {} (bank, cache_key, data) values(%s,%s,%s)".format(
        __context__["mysql_table_name"]
    )
    args = (bank, key, data)

    cur, cnt = run_query(__context__.get("mysql_client"), query, args=args)
    cur.close()
    if cnt not in (1, 2):
        raise SaltCacheError(f"Error storing {bank} {key} returned {cnt}")


def fetch(bank, key):
    """
    Fetch a key value.
    """
    _init_client()
    query = "SELECT data FROM {} WHERE bank=%s AND cache_key=%s".format(
        __context__["mysql_table_name"]
    )
    cur, _ = run_query(__context__.get("mysql_client"), query, args=(bank, key))
    r = cur.fetchone()
    cur.close()
    if r is None:
        return {}
    return salt.payload.loads(r[0])


def flush(bank, key=None):
    """
    Remove the key from the cache bank with all the key content.
    """
    _init_client()
    query = "DELETE FROM {} WHERE bank=%s".format(__context__["mysql_table_name"])
    if key is None:
        data = (bank,)
    else:
        data = (bank, key)
        query += " AND cache_key=%s"

    cur, _ = run_query(__context__.get("mysql_client"), query, args=data)
    cur.close()


def list_(bank):
    """
    Return an iterable object containing all entries stored in the specified bank.
    """
    _init_client()
    query = "SELECT bank FROM {}".format(__context__["mysql_table_name"])
    cur, _ = run_query(__context__.get("mysql_client"), query)
    out = [row[0] for row in cur.fetchall()]
    cur.close()
    minions = []
    for entry in out:
        minion = entry.replace('minions/', '')
        minions.append(minion)
    return minions


def contains(bank, key):
    """
    Checks if the specified bank contains the specified key.
    """
    _init_client()
    if key is None:
        data = (bank,)
        query = "SELECT COUNT(data) FROM {} WHERE bank=%s".format(__context__["mysql_table_name"])
    else:
        data = (bank, key)
        query = "SELECT COUNT(data) FROM {} WHERE bank=%s AND cache_key=%s".format(
            __context__["mysql_table_name"]
        )
    cur, _ = run_query(__context__.get("mysql_client"), query, args=data)
    r = cur.fetchone()
    cur.close()
    return r[0] == 1


def updated(bank, key):
    """
    Return the integer Unix epoch update timestamp of the specified bank and
    key.
    """
    _init_client()
    query = "SELECT UNIX_TIMESTAMP(last_update) FROM {} WHERE bank=%s " "AND cache_key=%s".format(
        __context__["mysql_table_name"]
    )
    data = (bank, key)
    cur, _ = run_query(__context__.get("mysql_client"), query=query, args=data)
    r = cur.fetchone()
    cur.close()
    return int(r[0]) if r else r