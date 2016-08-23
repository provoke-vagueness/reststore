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

