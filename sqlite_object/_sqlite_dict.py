from ._sqlite_object import SqliteObject
import json

try:
    unicode
except NameError:
    unicode = str


class SqliteDict(SqliteObject):
    """
    Dict-like object backed by an sqlite db.

    Make sure your keys serialize repeatably with whatever serializer you choose to use (the default is json).
    If you use un-ordered sets, the json serializer may sometimes generate different keys, so don't do that!

    Supports pretty much everything a regular dict supports:
    - setting values
    - retrieving values
    - checking if dict contains a key
    - iterations
    - get() and setdefault()
    - update(<another dict or list like [(key, value),]>)
    - pop() and popitem()
    """
    __schema = '''CREATE TABLE IF NOT EXISTS "{table_name}" (key TEXT PRIMARY KEY, value TEXT)'''
    __index = '''CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" (value)'''

    def __init__(self,
        init_dict={},
        filename=None,
        coder=json.dumps,
        decoder=json.loads,
        index_values=False,
        persist=False,
        commit_every=0,
        name=None,
        table_name_fmt='{name}_dict_table',
        index_name_fmt='{name}_dict_index',
    ):
        if not name:  # Compat
            self.table_name = 'dict'
            self.index_name = 'dict_index'
        else:
            self.table_name = table_name_fmt.format(name=name)
            self.index_name = index_name_fmt.format(name=name)

        schema_ddl = self.__schema.format( table_name=self.table_name )
        index_ddl  = self.__index.format(  table_name=self.table_name, index_name=self.index_name )

        super(SqliteDict, self).__init__(
            schema_ddl,
            index_ddl,
            filename,
            coder,
            decoder,
            index=index_values,
            persist=persist,
            commit_every=commit_every
        )

        for key, value in init_dict.items():
            self[key] = value

    def __len__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT COUNT(*) FROM "{table_name}"'''
                        .format(table_name=self.table_name)
                ):
                    return row[0]

    def __getitem__(self, key):
        with self.lock:
            if type(key) == slice:
                raise KeyError("Slices not allowed in SqliteDict")
            else:
                with self._closeable_cursor() as cursor:
                    cursor.execute(
                        '''SELECT value FROM "{table_name}" WHERE key = ?'''
                            .format(table_name=self.table_name), (self._coder(key), )
                    )
                    row = cursor.fetchone()

                    if row is not None:
                        return self._decoder(row[0])
                    else:
                        raise KeyError("Mapping key not found in dict")

    def __setitem__(self, key, value):
        with self.lock:
            if type(key) == slice:
                raise KeyError("Slices not allowed in SqliteDict")
            else:
                with self._closeable_cursor() as cursor:
                    cursor.execute(
                        '''REPLACE INTO "{table_name}" (key, value) VALUES (?, ?)'''
                            .format(table_name=self.table_name), (self._coder(key), self._coder(value))
                    )

            self._do_write()

    def __delitem__(self, key):
        with self.lock:
            if type(key) == slice:
                raise KeyError("Slices not allowed in SqliteDict")
            else:
                with self._closeable_cursor() as cursor:
                    cursor.execute(
                        '''DELETE FROM "{table_name}" WHERE key = ?'''
                            .format(table_name=self.table_name),
                        (self._coder(key),)
                    )

            self._do_write()

    def __iter__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT key FROM "{table_name}"'''
                        .format(table_name=self.table_name)
                ):
                    yield self._decoder(row[0])

    def __contains__(self, key):
        with self.lock:
            try:
                val = self[key]
            except KeyError:
                return False
            else:
                return True

    def clear(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''DELETE FROM "{table_name}"'''
                        .format(table_name=self.table_name)
                )

    def get(self, key, default=None):
        with self.lock:
            try:
                val = self[key]
            except KeyError:
                val = default
            return val

    def pop(self, key, default=None):
        with self.lock:
            val = self[key]
            del self[key]
            return val

    def popitem(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT key, value FROM "{table_name}" LIMIT 1'''
                        .format(table_name=self.table_name)
                )
                row = cursor.fetchone()

                if row is None:
                    raise KeyError("Dict has no more items to pop")
                else:
                    key = self._decoder(row[0])
                    value = self._decoder(row[1])
                    del self[key]
                    return (key, value)

            self._do_write()

    def setdefault(self, key, default=None):
        with self.lock:
            try:
                return self[key]
            except KeyError:
                self[key] = default
                return default

            self._do_write()

    def update(self, other=None, **kwargs):
        with self.lock:
            if "items" in dir(other):
                for key, value in other.items():
                    self[key] = value
            else:
                for key, value in other:
                    self[key] = value
            for key, value in kwargs:
                self[key] = value


    class ItemView(object):
        def __init__(self, sq_dict):
            self._sq_dict = sq_dict

        def __contains__(self, item):
            sq_dict = self._sq_dict

            key, value = item
            with sq_dict._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT * FROM "{table_name}" WHERE key = ? AND value = ?'''
                        .format(table_name=sq_dict.table_name),
                    (sq_dict._coder(key), sq_dict._coder(value))
                )
                val = cursor.fetchone()

                if val is None:
                    return False
                else:
                    return True

        def __iter__(self):
            sq_dict = self._sq_dict

            with sq_dict._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT key, value FROM "{table_name}"'''
                        .format(table_name=sq_dict.table_name)
                ):
                    yield sq_dict._decoder(row[0]), sq_dict._decoder(row[1])

    class KeyView(object):
        def __init__(self, sq_dict):
            self._sq_dict = sq_dict

        def __contains__(self, key):
            sq_dict = self._sq_dict

            with sq_dict._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT * FROM "{table_name}" WHERE key = ? '''
                        .format(table_name=sq_dict.table_name),
                    (sq_dict._coder(key), )
                )
                val = cursor.fetchone()

                if val is None:
                    return False
                else:
                    return True

        def __iter__(self):
            sq_dict = self._sq_dict

            with sq_dict._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT key FROM "{table_name}"'''
                        .format(table_name=sq_dict.table_name)
                ):
                    yield sq_dict._decoder(row[0])

    class ValueView(object):
        def __init__(self, sq_dict):
            self._sq_dict = sq_dict

        def __contains__(self, value):
            sq_dict = self._sq_dict

            with sq_dict._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT * FROM "{table_name}" WHERE value = ? '''
                        .format(table_name=sq_dict.table_name),
                    (sq_dict._coder(value), )
                )
                val = cursor.fetchone()

                return val is not None

        def __iter__(self):
            sq_dict = self._sq_dict

            with sq_dict._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT value FROM "{table_name}"'''
                        .format(table_name=sq_dict.table_name)
                ):
                    yield sq_dict._decoder(row[0])

    def items(self):
        return self.ItemView(self)

    def keys(self):
        return self.KeyView(self)

    def values(self):
        return self.ValueView(self)


    def write(self, outfile):
        with self.lock:
            outfile.write(u"{")
            iterator = iter(self.items())
            try:
                this = next(iterator)
            except StopIteration:
                outfile.write(u"}")
                return
            else:
                while True:
                    outfile.write(unicode(json.dumps(str(this[0]))))
                    outfile.write(u":")
                    outfile.write(unicode(json.dumps(this[1])))
                    try:
                        this = next(iterator)
                    except StopIteration:
                        outfile.write(u"}")
                        break
                    else:
                        outfile.write(u",")

    def write_lines(self, outfile, key_coder=json.dumps, value_coder=json.dumps, separator=u"\n", key_val_separator=u"\t"):
        with self.lock:
            iterator = iter(self.items())
            try:
                this = next(iterator)
            except StopIteration:
                return
            else:
                while True:
                    outfile.write(unicode(key_coder(this[0])))
                    outfile.write(unicode(key_val_separator))
                    outfile.write(unicode(value_coder(this[1])))
                    outfile.write(unicode(separator))
                    try:
                        this = next(iterator)
                    except StopIteration:
                        break
