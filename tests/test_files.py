import unittest
import shutil

import filestore


class TestFiles(unittest.TestCase):

    def setUp(self):
        t = filestore.Files(name='test_filestore')
        shutil.rmtree(t._root)
        t = filestore.Files(name='test_filestore')
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

