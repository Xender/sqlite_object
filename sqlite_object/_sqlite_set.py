import json
from ._sqlite_object import SqliteObject

try:
    unicode
except NameError:
    unicode = str


class SqliteSet(SqliteObject):
    __schema = '''CREATE TABLE IF NOT EXISTS "{table_name}" (key TEXT PRIMARY KEY)'''

    def __init__(self,
        init_set=[],
        filename=None,
        coder=json.dumps,
        decoder=json.loads,
        persist=False,
        commit_every=0,
        name=None,
        table_name_fmt='{name}_set_table',
    ):
        if not name:  # Compat
            self.table_name = 'set_table'
        else:
            self.table_name = table_name_fmt.format(name=name)

        schema_ddl = self.__schema.format( table_name=self.table_name )

        super(SqliteSet, self).__init__(
            schema_ddl,
            None,  # No index other than PK.
            filename,
            coder,
            decoder,
            index=False,
            persist=persist,
            commit_every=commit_every
        )

        for item in init_set:
            self.add(item)

    def _getlen(self, cursor):
        for row in cursor.execute(
            '''SELECT COUNT(*) FROM "{table_name}"'''
                .format(table_name=self.table_name)
        ):
            return row[0]

    def _has(self, cursor, item):
        rows = cursor.execute(
            '''SELECT key FROM "{table_name}" WHERE key = ?'''
                .format(table_name=self.table_name),
            (self._coder(item), )
        )
        return rows.fetchone() is not None

    def _remove(self, cursor, item):
        if self._has(cursor, item):
            self._discard(cursor, item)
        else:
            raise KeyError("Item not in \"{table_name}\"".format(self.table_name))

    def _discard(self, cursor, item):
        cursor.execute(
            '''DELETE FROM "{table_name}" WHERE key = ?'''
                .format(table_name=self.table_name),
            (self._coder(item), )
        )

    def _add(self, cursor, item):
        cursor.execute(
            '''INSERT OR IGNORE INTO "{table_name}" (key) VALUES (?)'''
                .format(table_name=self.table_name),
            (self._coder(item), )
        )

    def __len__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                return self._getlen(cursor)

    def __contains__(self, item):
        with self.lock:
            with self._closeable_cursor() as cursor:
                return self._has(cursor, item)

    def __iter__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT key FROM "{table_name}"'''
                        .format(table_name=self.table_name)
                ):
                    yield self._decoder(row[0])

    def add(self, item):
        with self.lock:
            with self._closeable_cursor() as cursor:
                self._add(cursor, item)

            self._do_write()

    def remove(self, item):
        with self.lock:
            with self._closeable_cursor() as cursor:
                self._remove(cursor, item)

            self._do_write()

    def discard(self, item):
        with self.lock:
            with self._closeable_cursor() as cursor:
                self._discard(cursor, item)

            self._do_write()

    def pop(self):
        out = None

        with self.lock:
            with self._closeable_cursor() as cursor:
                rows = cursor.execute(
                    '''SELECT key FROM "{table_name}" LIMIT 1'''
                        .format(table_name=self.table_name)
                )
                row = rows.fetchone()

                if row is None:
                    raise KeyError("Tried to pop empty set_table")

                self._discard(cursor, self._decoder(row[0]))
                out = self._decoder(row[0])

            self._do_write()
            return out


    def isdisjoint(self, other):
        return all(
            (item not in other)
            for item in self
        )


    def issubset(self, other):
        return all(
            (item in other)
            for item in self
        )


    def __le__(self, other):
        return self.issubset(other)

    def __lt__(self, other):
        return self.issubset(other) and (len(self) < len(other))

    def issuperset(self, other):
        return all(
            (item in self)
            for item in other
        )


    def __ge__(self, other):
        return self.issuperset(other)

    def __gt__(self, other):
        return self.issuperset(other) and (len(self) > len(other))

    def __eq__(self, other):
        if len(self) != len(other):
            return False

        return all(
            (item in other)
            for item in self
        )
    def update(self, other):
        for item in other:
            self.add(item)

    def clear(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''DELETE FROM "{table_name}"'''
                        .format(table_name=self.table_name)
                )

    def write(self, outfile):
        with self.lock:
            outfile.write(u"[")
            iterator = iter(self)

            try:
                this = next(iterator)
            except StopIteration:
                outfile.write(u"]")
                return
            else:
                while True:
                    outfile.write(unicode(json.dumps(this)))

                    try:
                        this = next(iterator)
                    except StopIteration:
                        outfile.write(u"]")
                        break
                    else:
                        outfile.write(u",")

    def write_lines(self, outfile, coder=json.dumps, separator=u"\n"):
        with self.lock:
            iterator = iter(self)
            try:
                this = next(iterator)
            except StopIteration:
                return
            else:
                while True:
                    outfile.write(unicode(coder(this)))
                    outfile.write(unicode(separator))

                    try:
                        this = next(iterator)
                    except StopIteration:
                        break
