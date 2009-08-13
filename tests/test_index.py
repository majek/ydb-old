import utils

big = "X" * 4096
def kv(prefix, chunks):
    for i in range(chunks):
        key = "key_%08i" % (i,)
        value = "%s_value_%08i_%s" % (prefix, i, big)
        yield key, value


class TestIndex(utils.YdbTest):
    @utils.provide_ydb(max_file_size=5*1024*1024)
    def test1(self, ydb):
        chunks = 1000
        for key, value in kv('#1', chunks):
            ydb.add(key, value)

        ydb = self.ydb_reopen()

        for key, value in kv('#1', chunks):
            self.assertEqual(ydb.get(key), value)

        for key, value in kv('#1', chunks):
            ydb.delete(key)

        self.assertEqual(ydb.get(key), None)
        ydb = self.ydb_reopen()
        self.assertEqual(ydb.get(key), None)

        for key, value in kv('#2', chunks):
            ydb.add(key, value)

        for key, value in kv('#3', chunks):
            ydb.add(key, value)

        for key, value in kv('#4', chunks/2):
            ydb.add(key, value)

        ydb = self.ydb_reopen()

        for key, value in kv('#4', chunks/2):
            self.assertEqual(ydb.get(key), value)

        for key, value in kv('#4', chunks/2):
            ydb.delete(key)


        self.assertEqual(0, 1)


if __name__ == '__main__':
    utils.run_unittests(globals())
