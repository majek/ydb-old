import utils

def kv_big(chunks):
    big_value = "x" * (4*1024*1024 - 128)
    for i in range(chunks):
        key = "key_%i" % (i,)
        value = big_value + key
        yield key, value

class TestRotation(utils.YdbTest):
    @utils.provide_ydb(max_file_size=6*1024*1024)
    def test1(self, ydb):
        chunks = 16
        for key, value in kv_big(chunks):
            ydb.add(key, value)

        for key, value in kv_big(chunks):
            self.assertEqual(ydb.get(key), value)

        ydb = self.ydb_reopen()

        for key, value in kv_big(chunks):
            self.assertEqual(ydb.get(key), value)



if __name__ == '__main__':
    utils.run_unittests(globals())
