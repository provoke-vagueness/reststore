import unittest
import shutil
import hashlib

import filestore


class TestFiles(unittest.TestCase):

    def setUp(self):
        t = filestore.Files(name='test_filestore')
        shutil.rmtree(t._root)
        t = filestore.Files(name='test_filestore')
        self.files = t

    def test_set_and_get(self):
        data = 'abcdefg'
        digest = hashlib.md5(data).hexdigest()
        self.files[digest] = data
        self.files[digest] = data
        self.files.put(data)
        self.assertRaises(ValueError,
                self.files.put,
                'abcdef', hexdigest=digest)

        rdata = self.files[digest]
        self.assertRaises(KeyError,
                self.files.__getitem__,
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')

