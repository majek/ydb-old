import utils
import random

def kv_big(chunks):
    big_value = "x" * 1024
    for i in range(chunks):
        key = "key_%i" % (i,)
        value = ('%i%s' % (i, big_value)) [:1024]
        yield key, value

class TestGC(utils.YdbTest):
    @utils.provide_ydb(min_log_size=1024)
    def test1(self, ydb):
        chunks = 32
        def write(chunks, garbage=False):
            for key, value in kv_big(chunks):
                if garbage:
                    # signgle item, that will not be moved or overwritten
                    ydb.add('%f' % random.random() , 'a')
                ydb.add(key, value)

            for key, value in kv_big(chunks):
                self.assertEqual(ydb.get(key), value)

        # create 32 logs.
        write(chunks, True)

        # overwrite them.
        write(chunks, True)

        ydb = self.ydb_reopen()
        self.ydb.sync()

        # and overwrite again
        write(chunks, True)

        ydb = self.ydb_reopen()

        # and again
        write(chunks, True)

        # and desroy :)
        for key, value in kv_big(chunks):
            ydb.delete(key)

        ydb = self.ydb_reopen()

        for key, value in kv_big(chunks):
            self.assertEqual(ydb.get(key), None)

        #self.assertEqual(0, 1)


if __name__ == '__main__':
    utils.run_unittests(globals())
