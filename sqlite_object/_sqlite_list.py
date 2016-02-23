from ._sqlite_object import SqliteObject
import json

"""
from sqlite_object import  SqliteList
l = SqliteList()
l.append("hi")
l.append("another one!")
"""

try:
    unicode
except NameError:
    unicode = str


class SqliteList(SqliteObject):
    """
    List-like object backed by an on-disk SQL db

    Supports:
    - Indexing
    - Slicing (fairly efficient, but not insanely so)
    - Overwriting list elements
    - Adding items to either end of the list
    - Removing items from either end of the list
    - Checking if the list contains an item
    - Efficient iteration over the whole list (forward and reversed())

    Doesn't support:
    - Inserting items into the middle of the list
    - Deleting items from the middle of the list
    """

    __schema = '''CREATE TABLE IF NOT EXISTS "{table_name}" (list_index INTEGER PRIMARY KEY, value TEXT)'''
    __index = '''CREATE INDEX IF NOT EXISTS "{index_name}" ON "{table_name}" (value)'''

    def __init__(self,
        init_list=[],
        filename=None,
        coder=json.dumps,
        decoder=json.loads,
        index=True,
        persist=False,
        commit_every=0,
        name=None,
        table_name_fmt='{name}_list_table',
        index_name_fmt='{name}_list_index',
    ):
        if not name:  # Compat
            self.table_name = 'list'
            self.index_name = 'list_value'
        else:
            self.table_name = table_name_fmt.format(name=name)
            self.index_name = index_name_fmt.format(name=name)

        schema_ddl = self.__schema.format( table_name=self.table_name )
        index_ddl  = self.__index.format(  table_name=self.table_name, index_name=self.index_name )

        super(SqliteList, self).__init__(
            schema_ddl,
            index_ddl,
            filename,
            coder,
            decoder,
            index=index,
            persist=persist,
            commit_every=commit_every
        )

        for item in init_list:
            self.append(item)

    def _getlen(self, cursor):
        for row in cursor.execute(
            '''SELECT COUNT(*) FROM "{table_name}"'''
                .format(table_name=self.table_name)
        ):
            return row[0]

    def _getmin(self, cursor):
        for row in cursor.execute(
            '''SELECT MIN(list_index) FROM "{table_name}"'''
                .format(table_name=self.table_name)
        ):
            return row[0]

    def _getmax(self, cursor):
        for row in cursor.execute(
            '''SELECT MAX(list_index) FROM "{table_name}"'''
                .format(table_name=self.table_name)
        ):
            return row[0]

    def _getitem(self, cursor, item):
        for row in cursor.execute(
            '''SELECT value FROM "{table_name}" WHERE list_index = (SELECT MIN(list_index) FROM "{table_name}") + ?'''
                .format(table_name=self.table_name),
            (item, )
        ):
            return self._decoder(row[0])

    def __len__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                return self._getlen(cursor)

    def _minindex(self):
        with self.lock:
            #find the lowest index
            with self._closeable_cursor() as cursor:
                for row in cursor.execute(
                    '''SELECT MIN(list_index) FROM "{table_name}"'''
                        .format(table_name=self.table_name)
                ):
                    return row[0]

            #if there's nothing in the list, return 0
            return 0

    def _iterate(self, length, irange):
        for i in irange:
            if i >= 0 and i<length:
                yield self[i]

    def __getitem__(self, key):
        with self.lock:
            with self._closeable_cursor() as cursor:
                length = self._getlen(cursor)

            if type(key) != int:
                if type(key) == slice:
                    #start = key.start or (0 if key.step > 0 else max(length, 0))
                    #stop = key.stop or (length if key.step > 0 else 0)
                    #step = key.step or 1
                    #if start < 0:
                    #    start = length + start
                    #if stop < 0:
                    #    stop = length + stop
                    ##if step < 0:
                    ##    tmp = start
                    ##    start = max(stop - 1, 0)
                    ##    stop = tmp
                    return (self._iterate(length, range(length)[key.start:key.stop:key.step]))
                else:
                    raise TypeError("Key should be int, got " + str(type(key)))

            elif key >= length:
                raise IndexError("Sequence index out of range.")

            else:
                with self._closeable_cursor() as cursor:
                    if key < 0:
                        key = length + key
                        if key >= length:
                            raise IndexError("Sequence index out of range.")
                        if key < 0:
                            raise IndexError("Sequence index out of range.")

                    cursor.execute(
                        '''SELECT value FROM "{table_name}" WHERE list_index = (SELECT MIN(list_index) FROM "{table_name}") + ?'''
                            .format(table_name=self.table_name),
                        (key, )
                    )
                    return self._decoder(cursor.fetchone()[0])

    def __setitem__(self, key, value):
        with self.lock:
            if type(key) != int:
                raise TypeError("Key should be int, got " + str(type(key)))

            with self._closeable_cursor() as cursor:
                if key < 0:
                    key = len(self) + key
                    if key < 0:
                        raise IndexError("Sequence index out of range.")

                if key >= len(self):
                    raise IndexError("Sequence index out of range.")

                cursor.execute(
                    '''REPLACE INTO list (list_index, value) VALUES ((SELECT MIN(list_index) FROM "{table_name}") + ?, ?)'''
                        .format(table_name=self.table_name),
                    (key, self._coder(value))
                )

            self._do_write()

    def __iter__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT value FROM "{table_name}" ORDER BY list_index ASC'''
                        .format(table_name=self.table_name)
                )

                for row in cursor:
                    yield self._decoder(row[0])

    def __reversed__(self):
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT value FROM "{table_name}" ORDER BY list_index DESC'''
                        .format(table_name=self.table_name)
                )

                for row in cursor:
                    yield self._decoder(row[0])

    def __contains__(self, item):
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''SELECT list_index FROM "{table_name}" WHERE value = ?'''
                        .format(table_name=self.table_name), (self._coder(item), )
                )

                return cursor.fetchone() is not None

    def append(self, item):
        """
        Add an item to the end of the list
        """
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO "{table_name}" (list_index, value) VALUES ((SELECT MAX(list_index) FROM "{table_name}") + 1, ?)'''
                        .format(table_name=self.table_name),
                    (self._coder(item), )
                )

            self._do_write()

    def prepend(self, item):
        """
        Insert an item at the front of the list
        """
        with self.lock:
            with self._closeable_cursor() as cursor:
                cursor.execute(
                    '''INSERT INTO "{table_name}" (list_index, value) VALUES ((SELECT MIN(list_index) FROM "{table_name}") - 1, ?)'''
                        .format(table_name=self.table_name),
                    ( self._coder(item), )
                )

            self._do_write()

    def pop_last(self):
        with self.lock:
            output = None

            with self._closeable_cursor() as cursor:
                cursor.execute('''BEGIN TRANSACTION''')

                if self._getlen(cursor) < 1:
                    cusror.execute('''END TRANSACTION''')
                    raise IndexError("pop from empty list")

                cursor.execute(
                    '''SELECT value FROM "{table_name}" WHERE list_index = (SELECT MAX(list_index) FROM "{table_name}")'''
                        .format(table_name=self.table_name)
                )
                output = self._decoder( cursor.fetchone()[0] )

                cursor.execute(
                    '''DELETE FROM "{table_name}" WHERE list_index = (SELECT MAX(list_index) FROM "{table_name}")'''
                        .format(table_name=self.table_name)
                )

                self._db.commit()

            self._do_write()
            return output

    def pop_first(self):
        with self.lock:
            output = None

            with self._closeable_cursor() as cursor:
                cursor.execute('''BEGIN TRANSACTION''')

                if self._getlen(cursor) < 1:
                    cusror.execute('''END TRANSACTION''')
                    raise IndexError("pop from empty list")

                cursor.execute(
                    '''SELECT value FROM "{table_name}" WHERE list_index = (SELECT MIN(list_index) FROM "{table_name}")'''
                        .format(table_name=self.table_name)
                )
                output = self._decoder(cursor.fetchone()[0])

                cursor.execute(
                    '''DELETE FROM "{table_name}" WHERE list_index = (SELECT MIN(list_index) FROM "{table_name}")'''
                        .format(table_name=self.table_name)
                )

                self._db.commit()

            self._do_write()
            return output

    def extend(self, iterable):
        """
        Add each item from iterable to the end of the list
        """
        with self.lock:
            for item in iterable:
                self.append(item)

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
