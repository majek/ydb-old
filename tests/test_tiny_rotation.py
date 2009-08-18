import utils

def kv_big(chunks):
    big_value = "v" * 128
    for i in range(chunks):
        key = "key_%i" % (i,)
        value = '%i%s%i' % (i, big_value, i)
        yield key, value

class TestTinyRotation(utils.YdbTest):
    @utils.provide_ydb(min_log_size=1)
    def test1(self, ydb):
        chunks = 32
        def write(chunks, garbage=False):
            for key, value in kv_big(chunks):
                if garbage:
                    ydb.add('a'+key, 'a') # signgle add to trigger gc
                ydb.add(key, value)

            for key, value in kv_big(chunks):
                self.assertEqual(ydb.get(key), value)

        # create 32 logs.
        write(chunks, True)

        # overwrite them.
        write(chunks)

        ydb = self.ydb_reopen()
        self.ydb.sync()

        # and overwrite again
        write(chunks)

        ydb = self.ydb_reopen()

        # and again
        write(chunks)

        # and desroy :)
        for key, value in kv_big(chunks):
            ydb.delete(key)

        ydb = self.ydb_reopen()

        for key, value in kv_big(chunks):
            self.assertEqual(ydb.get(key), None)


        #self.assertEqual(0, 1)


if __name__ == '__main__':
    utils.run_unittests(globals())
