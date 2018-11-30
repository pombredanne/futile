import os
import threading
from datetime import datetime
import MySQLdb as mysql
from queue import LifoQueue, Full, Empty
from DBUtils.PersistentDB import PersistentDB
from futile.strings import ensure_str


class ConnectionError(Exception):
    pass


def _quote(s):
    return "'" + mysql.escape_string(str(s)).decode("utf-8") + "'"


def _quote_key(s):
    return "`" + s + "`"


def _dict2str(dictin, joiner=", "):
    # in sql, where key='value' or key in (value), dicts are the values to update
    sql = []
    for k, v in dictin.items():
        if isinstance(v, (list, tuple)):
            part = f"`{k}` in ({','.join(map(_quote, v))})"
        else:
            part = f"`{k}`={_quote(v)}"
        sql.append(part)
    return joiner.join(sql)


class Connection:
    def execute(self, query):
        self.conn.ping(True)
        self.cursor.execute(query)
        return self.cursor.fetchall()


class ConnectionPool:
    def __init__(
        self,
        max_connections=50,
        timeout=20,
        connection_factory=None,
        queue_class=LifoQueue,
        **connection_kwargs,
    ):
        self.max_connections = max_connections
        self.connection_factory = connection_factory
        self.queue_class = queue_class
        self.timeout = timeout

        self.reset()

    def _checkpid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited on the lock.
                    return
                self.disconnect()
                self.reset()

    def reset(self):
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

    def make_connection(self):
        """Make a fresh connection."""
        connection = self.connection_factory(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.
        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        return connection

    def release(self, connection):
        """
        Releases the connection back to the pool.
        """
        # Make sure we haven't changed process.
        self._checkpid()
        if connection.pid != self.pid:
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        "Disconnects all connections in the pool."
        for connection in self._connections:
            connection.disconnect()


class MysqlDatabase:
    def __init__(self, client, dry_run=False):
        self._client = client
        self._dry_run = dry_run

    def connect(self):
        self._client = mysql.connect(**self._connect_params)

    def query(self, stmt):
        conn = self._client.connection()
        cursor = conn.cursor(mysql.cursors.DictCursor)
        cursor.execute(stmt)
        conn.commit()
        return cursor

    def create_table(self, table, fields, indexes=None, unique=None):
        sql = [
            "create table if not exists ",
            table,
            "(id bigint unsigned not null primary key auto_increment,",
        ]
        for field_name, field_type in fields:
            sql.append(_quote_key(field_name))
            sql.append(field_type)
            sql.append(",")
        if indexes:
            for index in indexes:
                sql.append("index")
                if isinstance(index, str):
                    index = [index]
                sql.append(
                    "idx_%s(%s)" % ("_".join(index), ",".join(map(_quote_key, index)))
                )
                sql.append(",")
        if unique:
            for uniq in unique:
                sql.append("unique")
                if isinstance(uniq, str):
                    uniq = [uniq]
                sql.append(
                    "uniq_%s(%s)" % ("_".join(uniq), ",".join(map(_quote_key, uniq)))
                )
                sql.append(",")
        sql.pop()
        sql.append(
            ") Engine=InnoDB default charset=utf8mb4 collate utf8mb4_general_ci;"
        )
        stmt = " ".join(sql)
        if self._dry_run:
            print(stmt)
        else:
            return self.query(stmt)

    def insert_or_update(self, table, defaults, **where):
        """
        insert into table (key_list) values (value_list) on duplicate key update (value_list)
        """
        insertion = {**defaults, **where}
        fields = ",".join(map(_quote_key, insertion.keys()))
        values = ",".join([_quote(v) for v in insertion.values()])
        updates = _dict2str(defaults)
        tmpl = "insert into %s (%s) values (%s) on duplicate key update %s"
        stmt = tmpl % (table, fields, values, updates)
        if self._dry_run:
            print(stmt)
        else:
            return self.query(stmt)

    def insert(self, table, defaults):
        fields = ",".join(map(_quote_key, defaults.keys()))
        values = ",".join(map(_quote, defaults.values()))
        tmpl = "insert into %s (%s) values (%s)"
        stmt = tmpl % (table, fields, values)
        if self._dry_run:
            print(stmt)
        else:
            return self.query(stmt)

    def update(self, table, defaults, **where):
        tmpl = "update %s set %s where %s"
        stmt = tmpl % (table, _dict2str(defaults), _dict2str(where, " and "))
        if self._dry_run:
            print(stmt)
        else:
            return self.query(stmt)

    def select(self, table, keys="*", where=None, limit=None, offset=None):
        if isinstance(keys, (tuple, list)):
            keys = ",".join(keys)
        tmpl = "select %s from %s"
        sql = [tmpl % (keys, table)]
        if where:
            sql.append("where")
            sql.append(_dict2str(where, " and "))
        if limit:
            sql.append("limit")
            sql.append(str(limit))
        if offset:
            sql.append("offset")
            sql.append(str(offset))

        stmt = " ".join(sql)

        if self._dry_run:
            print(stmt)
        else:
            return self.query(stmt)


def main():
    db = MysqlDatabase(None, dry_run=True)
    db.create_table(
        "alibaba_deal_info",
        [("product_id", "varchar(128)")],
        [["product_id", "city"], ["id"]],
    )
    db.insert("alibaba_deal_info", {"foo": "bar", "a": "b"})
    db.select("alibaba_product", where={"product_id": 1}, limit=5, offset=5)


if __name__ == "__main__":
    main()
