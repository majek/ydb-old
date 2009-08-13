import utils

class TestReload(utils.YdbTest):
    @utils.provide_ydb(max_file_size=6*1024*1024)
    def test1(self, ydb):
        key1 = 'a'
        key2 = 'b'
        value1 = 'a' * 4*1024*1024
        value2 = 'b' * 4*1024*1024
        ydb.add(key1, value1)
        ydb.add(key2, value2)
        self.assertEqual(ydb.get(key1), value1, 'a1')
        self.assertEqual(ydb.get(key2), value2, 'b1')

        ydb = self.ydb_reopen()

        self.assertEqual(ydb.get(key1), value1, 'a2')
        self.assertEqual(ydb.get(key2), value2, 'b2')

    @utils.provide_ydb(max_file_size=6*1024*1024)
    def test_del(self, ydb):
        key1 = 'a'
        key2 = 'b'
        key3 = 'c'
        value1 = 'a' * 4*1024*1024
        value2 = 'b' * 4*1024*1024
        value3 = 'c' * 4*1024*1024
        ydb.add(key1, value1)
        ydb.add(key2, value2)
        ydb.add(key3, value3)

        ydb.delete(key1)
        ydb = self.ydb_reopen()

        self.assertEqual(ydb.get(key1), None, 'a3')
        self.assertEqual(ydb.get(key2), value2, 'b3')
        self.assertEqual(ydb.get(key3), value3, 'c3')

        ydb.delete(key2)
        ydb = self.ydb_reopen()

        self.assertEqual(ydb.get(key1), None, 'a4')
        self.assertEqual(ydb.get(key2), None, 'b4')
        self.assertEqual(ydb.get(key3), value3, 'c3')

if __name__ == '__main__':
    utils.run_unittests(globals())
