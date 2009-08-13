import utils

class TestBasic(utils.YdbTest):
    @utils.provide_ydb()
    def test_add(self, ydb):
        ydb.add("key", "b")
        self.assertEqual(ydb.get("key"), "b")

    @utils.provide_ydb()
    def test_overwrite(self, ydb):
        for i in range(500):
            ydb.add("key", "%i" % i)
        self.assertEqual(ydb.get("key"), "499")

    @utils.provide_ydb()
    def test_del(self, ydb):
        ydb.add("key", "b")
        self.assertEqual(ydb.get("key"), "b")
        ydb.delete("key")
        self.assertEqual(ydb.get("key"), None)


class TestKeySizes(utils.YdbTest):
    @utils.provide_ydb()
    def test_empty_key(self, ydb):
        key = ""
        ydb.add(key, "b1")
        self.assertEqual(ydb.get(key), "b1")

    @utils.provide_ydb()
    def test_medium_key(self, ydb):
        key = "a" * 256
        ydb.add(key, "b2")
        self.assertEqual(ydb.get(key), "b2")

    @utils.provide_ydb()
    def test_big_key1(self, ydb):
        key = "a" * 16384
        ydb.add(key, "b3")
        self.assertEqual(ydb.get(key), "b3")

    @utils.provide_ydb()
    def test_big_key2(self, ydb):
        key = "a" * 32768
        ydb.add(key, "b4")
        self.assertEqual(ydb.get(key), "b4")

    @utils.provide_ydb()
    def test_big_key3(self, ydb):
        key = "a" * 65535
        ydb.add(key, "b5")
        self.assertEqual(ydb.get(key), "b5")


class TestValueSizes(utils.YdbTest):
    @utils.provide_ydb()
    def test_empty_value(self, ydb):
        key = "v1"
        value = ""
        ydb.add(key, value)
        self.assertEqual(ydb.get(key), value)

    @utils.provide_ydb()
    def test_big_value1(self, ydb):
        key = "v2"
        value = "x" * (1024*1024)
        ydb.add(key, value)
        self.assertEqual(ydb.get(key), value)

    @utils.provide_ydb()
    def test_empty_value(self, ydb):
        key = "v3"
        value = "x" * (4*1024*1024)
        ydb.add(key, value)
        self.assertEqual(ydb.get(key), value)

class TestCornerKeyValueSizes(utils.YdbTest):
    @utils.provide_ydb()
    def test_empty(self, ydb):
        key = ""
        value = ""
        ydb.add(key, value)
        self.assertEqual(ydb.get(key), value)

    @utils.provide_ydb()
    def test_huge(self, ydb):
        key = "k" * 65535
        value = "x" * (4*1024*1024)
        ydb.add(key, value)
        self.assertEqual(ydb.get(key), value)


if __name__ == '__main__':
    utils.run_unittests(globals())
