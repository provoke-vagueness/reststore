import unittest
import shutil

import reststore


class TestFiles(unittest.TestCase):

    def setUp(self):
        t = reststore.Files(name='test_reststore')
        shutil.rmtree(t._root)
        t = reststore.Files(name='test_reststore')
        self.files = t

    def test_set_and_get(self):
        data = 'abcdefg'
        digest = self.files.hash_func(data).hexdigest()
        self.files[digest] = data
        self.files[digest] = data
        self.files.put(data)
        self.assertRaises(ValueError,
                self.files.put,
                'abcdef', hexdigest=digest)

        self.assertTrue(digest in self.files)
        self.assertFalse('asdfasdfa' in self.files)

        rdata = self.files[digest]
        self.assertRaises(KeyError,
                self.files.__getitem__,
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')

    def test_iterator(self):
        digest1 = self.files.put('abcdefg')
        digest2 = self.files.put('1234567')
        digest3 = self.files.put('foobar')

        for i, d in enumerate(self.files, 1):
            if i == 1:
                self.assertEquals(d, digest1)
            if i == 2:
                self.assertEquals(d, digest2)
            if i == 3:
                self.assertEquals(d, digest3)
            else:
                assert "Unexpected element returned"

    def test_select(self):
        digest1 = self.files.put('abcdefg')
        digest2 = self.files.put('1234567')
        digest3 = self.files.put('foobar')

        self.assertEquals([digest1], self.files.select(0, 1))
        self.assertEquals([digest1, digest2], self.files.select(0, 2))
        self.assertEquals([digest3], self.files.select(-2, -1))

    def test_len(self):
        self.assertFalse(len(self.files))
        digest1 = self.files.put('abcdefg')
        digest2 = self.files.put('1234567')
        digest3 = self.files.put('foobar')

        self.assertEquals(3, len(self.files))

    def test_expire(self):
        digest1 = self.files.put('abcdefg')
        digest2 = self.files.put('1234567')
        digest3 = self.files.put('foobar')
        self.files.expire(1)

        self.assertEquals(len(self.files), 2)

        for i, d in enumerate(self.files, 1):
            if i == 1:
                self.assertEquals(d, digest2)
            if i == 2:
                self.assertEquals(d, digest3)
            else:
                assert "Unexpected element returned"

        self.assertEquals([digest2], self.files.select(0, 1))
        self.assertEquals([digest2, digest3], self.files.select(0, 2))
        self.assertEquals([digest3], self.files.select(-2, -1))

    def test_concurrent_puts(self):
        """
        Attempt to trigger issues seen with concurrent puts.
        * Database locking on inserts
        * Race condition when two procs try to put same data/hash
        """
        import multiprocessing
        digest = self.files.hash_func('a').hexdigest()

        p = multiprocessing.Pool(3)
        res = p.map(subproc_put, ['a', 'a', 'a'])
        self.assertEquals(res, [digest, digest, digest])


def subproc_put(val):
    return reststore.Files(name='test_reststore').put(val)

