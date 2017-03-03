#!/usr/bin/env python

import unittest
from collections import Counter
import random
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


class ConnectedComponentTest(MyriaTestBase):
    # TODO: add failure injector and prioritization
    MAXN = 50
    MAXM = MAXN * 10

    def get_program(self, inputfile):
        return """
E = load("file://%s", csv(schema(src:int, dst:int), skip=0));
V = [from E emit src as x] + [from E emit dst as x];
V = select distinct x from V;
do
  CC = [nid, MIN(cid) as cid] <-
    [from V emit V.x as nid, V.x as cid] +
    [from E, CC where E.src = CC.nid emit E.dst as nid, CC.cid];
until convergence pull_idb;
store(CC, CC_output);
""" % inputfile

    def get_expected(self, edges):
        ans = {}
        for edge in edges:
            ans[edge[0]] = edge[0]
            ans[edge[1]] = edge[1]
        while True:
            changed = False
            for edge in edges:
                if ans[edge[0]] < ans[edge[1]]:
                    changed = True
                    ans[edge[1]] = ans[edge[0]]
            if not changed: break
        return [{'nid':k, 'cid':ans[k]} for k in ans]

    def test(self):
        edges = []
        for i in range(self.MAXM):
            src = random.randint(0, self.MAXN)
            dst = random.randint(0, self.MAXN)
            edges.append((src, dst))
        with NamedTemporaryFile(suffix='.csv') as cc_input:
            for edge in edges:
                cc_input.write('%d,%d\n' % (edge[0], edge[1]))
            cc_input.flush()
            query = MyriaQuery.submit(self.get_program(cc_input.name))
            self.assertEqual(query.status, 'SUCCESS')
            results = MyriaRelation('public:adhoc:CC_output').to_dict()
            self.assertListOfDictsEqual(results, self.get_expected(edges))


class IngestEmptyQueryTest(MyriaTestBase):
    def test(self):
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
        data = "foo,3242\n" \
               "bar,321\n"\
               "baz,104"
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


class BadDelimiterTest(MyriaTestBase):
    def test(self):
        #TODO change URL to local file
        program = """
r = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/simple_two_col_int.txt',
                      csv(schema(x:int, y:int), delimiter=' '));
store(r, r);
"""
        query = MyriaQuery.submit(program)
        self.assertEqual(query.status, 'ERROR')


if __name__ == '__main__':
    unittest.main()
