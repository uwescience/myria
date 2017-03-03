#!/usr/bin/env python

import unittest
from collections import Counter
from tempfile import NamedTemporaryFile
from myria import MyriaConnection, MyriaRelation, MyriaQuery, MyriaError


class MyriaTestBase(unittest.TestCase):
    def setUp(self):
        connection = MyriaConnection(hostname='localhost', port=8753, execution_url="http://127.0.0.1:8080")
        MyriaRelation.DefaultConnection = connection
        self.connection = connection

    def assertListOfDictsEqual(self, left, right):
        self.assertEqual(Counter([kv for d in left for kv in d.items()]),
                         Counter([kv for d in right for kv in d.items()]))

class DoWhileTest(MyriaTestBase):
    def test(self):
        program = """
x = [0 as exp, 1 as val];
do
  x = [from x emit exp+1 as exp, 2*val as val];
while [from x emit max(exp) < 5];
store(x, powersOfTwo);
"""
        query = MyriaQuery.submit(program)
        expected = [{'val': 32, 'exp': 5}]

        self.assertEqual(query.status, 'SUCCESS')
        self.assertListOfDictsEqual(query.to_dict(), expected)


class IngestEmptyQueryTest(MyriaTestBase):
    def test(self):
        # Create empty file
        with NamedTemporaryFile() as f:
            #TODO change URL to local file
            program = """
emptyrelation = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/emptyrelation', csv(schema(foo:string, bar:int)));
store(emptyrelation, emptyrelation);
"""
        expected = []
        query = MyriaQuery.submit(program)
        self.assertEqual(query.status, 'SUCCESS')
        self.assertListOfDictsEqual(query.to_dict(), expected)


class UploadAndReplaceDataTest(MyriaTestBase):
    def test(self):
        with NamedTemporaryFile() as f:
            data = "foo,3242\n" \
                   "bar,321\n"\
                   "baz,104"
            f.write(data)
            #TODO change URL to local file
            program = """
uploaddatatest = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/uploaddatatest', csv(schema(s:string, i:int)));
store(uploaddatatest, uploaddatatest);
"""
            expected = [{u's': u'foo', u'i': 3242},
                        {u's': u'bar', u'i': 321},
                        {u's': u'baz', u'i': 104}]
            query = MyriaQuery.submit(program)
            self.assertEqual(query.status, 'SUCCESS')
            self.assertListOfDictsEqual(query.to_dict(), expected)

            # Now replace with another relation
            program = """
uploaddatatest = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/uploaddatatest2',
                       csv(schema(s:string, i:int), delimiter='\\t'));
store(uploaddatatest, uploaddatatest);
"""
            expected = [{u's': u'mexico', u'i': 42},
                        {u's': u'poland', u'i': 12342},
                        {u's': u'belize', u'i': 802304}]
            query = MyriaQuery.submit(program)
            self.assertEqual(query.status, 'SUCCESS')
            self.assertListOfDictsEqual(query.to_dict(), expected)

class KillQueryTest(MyriaTestBase):
    def test(self):
        # This is a long-running program
        program = """
x = [0 as exp, 1 as val];
do
  x = [from x emit exp+1 as exp, 2*val as val];
while [from x emit max(exp) < 1000];
store(x, powersOfTwo);
"""
        plan = self.connection.compile_program(program)
        # We submit with submit_plan so as to not block
        query = MyriaQuery.submit_plan(plan)

        # We kill the query and make sure it was killed
        query.kill()
        self.assertEqual(query.status, 'KILLED')



if __name__ == '__main__':
    unittest.main()
