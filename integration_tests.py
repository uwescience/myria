#!/usr/bin/env python

import random
import json
import unittest
from tempfile import NamedTemporaryFile
from collections import Counter
from myria import MyriaConnection, MyriaRelation, MyriaQuery, MyriaError
from sets import Set


class MyriaTestBase(unittest.TestCase):
    def setUp(self):
        connection = MyriaConnection(hostname='localhost', port=8753, execution_url="http://127.0.0.1:8080")
        MyriaRelation.DefaultConnection = connection
        self.connection = connection

    def assertListOfDictsEqual(self, left, right):
        self.assertEqual(Counter([tuple(sorted(d.items())) for d in left]),
                         Counter([tuple(sorted(d.items())) for d in right]))


class ListOfDictsEqualTest(MyriaTestBase):
    def test(self):
        left, right = ([{'a': 1, 'b': 2}, {'a': 2, 'b': 1}],
                       [{'a': 1, 'b': 2}, {'a': 2, 'b': 1}])
        self.assertListOfDictsEqual(left, right)

        left, right = ([{'a': 1, 'b': 1}, {'a': 1, 'b': 1}],
                       [{'a': 1, 'b': 1}, {'a': 1, 'b': 1}])
        self.assertListOfDictsEqual(left, right)

        left, right = ([{'a': 2, 'b': 1}, {'a': 2, 'b': 1}],
                       [{'a': 1, 'b': 2}, {'a': 2, 'b': 1}])
        self.assertRaises(self.assertListOfDictsEqual(left, right))

        left, right = ([{'a': 1, 'b': 2, 'c': 3}, {'a': 2, 'b': 1}],
                       [{'a': 1, 'b': 2}, {'a': 2, 'b': 1}])
        self.assertListOfDictsEqual(left, right)


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
        expected = [{'exp': 5, 'val': 32}]

        self.assertEqual(query.status, 'SUCCESS')
        self.assertListOfDictsEqual(query.to_dict(), expected)


class IngestEmptyQueryTest(MyriaTestBase):
    def test(self):
        # Create empty file
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


class ConnectedComponentTest(MyriaTestBase):
    # TODO: add failure injector and prioritization
    MAXN = 50
    MAXM = MAXN * 10

    def get_program(self, inputfile, sync_mode=''):
        return """
E = load("file://%s", csv(schema(src:int, dst:int), skip=0));
V = [from E emit src as x] + [from E emit dst as x];
V = select distinct x from V;
do
  CC = [nid, MIN(cid) as cid] <-
    [from V emit V.x as nid, V.x as cid] +
    [from E, CC where E.src = CC.nid emit E.dst as nid, CC.cid];
until convergence %s pull_idb;
store(CC, CC_output);
""" % (inputfile, sync_mode)

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
            src = random.randint(0, self.MAXN-1)
            dst = random.randint(0, self.MAXN-1)
            edges.append((src, dst))
        with NamedTemporaryFile(suffix='.csv') as cc_input:
            for edge in edges:
                cc_input.write('%d,%d\n' % (edge[0], edge[1]))
            cc_input.flush()
            query = MyriaQuery.submit(self.get_program(cc_input.name))
            self.assertEqual(query.status, 'SUCCESS')
            results = MyriaRelation('public:adhoc:CC_output').to_dict()
            self.assertListOfDictsEqual(results, self.get_expected(edges))
            query = MyriaQuery.submit(self.get_program(cc_input.name, 'sync'))
            self.assertEqual(query.status, 'SUCCESS')
            results = MyriaRelation('public:adhoc:CC_output').to_dict()
            self.assertListOfDictsEqual(results, self.get_expected(edges))


class JoinChainTest(MyriaTestBase):
    # TODO: add failure injector and prioritization
    MAXN = 20
    MAXM = 40

    def get_join_chain_program(self, R, A0, B0, C0):
        return """
R = load("file://%s", csv(schema(src:int, dst:int), skip=0));
A0 = load("file://%s", csv(schema(src:int, dst:int), skip=0));
B0 = load("file://%s", csv(schema(src:int, dst:int), skip=0));
C0 = load("file://%s", csv(schema(src:int, dst:int), skip=0));
do
  A = [src, dst] <-
    [from A0 emit src, dst] +
    [from R, A where R.dst = A.src emit R.src, A.dst];
  B = [src, dst] <-
    [from B0 emit src, dst] +
    [from A, B where A.dst = B.src emit A.src, B.dst];
  C = [src, dst] <-
    [from C0 emit src, dst] +
    [from B, C where B.dst = C.src emit B.src, C.dst];
until convergence;
store(C, joinChain);
""" % (R, A0, B0, C0)

    def get_join_chain_expected(self, R, A0, B0, C0):
	A = Set(A0)
	B = Set(B0)
	C = Set(C0)
        while True:
            change = False
	    for i in range(self.MAXN):
	        for j in range(self.MAXN):
	            for k in range(self.MAXN):
                        if (j, i) in R and (i, k) in A and (j, k) not in A:
                            change = True
                            A.add((j, k))
                        if (j, i) in A and (i, k) in B and (j, k) not in B:
                            change = True
                            B.add((j, k))
                        if (j, i) in B and (i, k) in C and (j, k) not in C:
                            change = True
                            C.add((j, k))
            if not change: break
        return [{'src':x, 'dst':y} for x, y in C]

    def genData(self, fin):
        edges = []
        for i in range(self.MAXM):
            src = random.randint(0, self.MAXN-1)
            dst = random.randint(0, self.MAXN-1)
            edges.append((src, dst))
            fin.write('%d,%d\n' % (src, dst))
        fin.flush()
        return edges

    def test(self):
        with NamedTemporaryFile(suffix='.csv') as R, NamedTemporaryFile(suffix='.csv') as A0, NamedTemporaryFile(suffix='.csv') as B0, NamedTemporaryFile(suffix='.csv') as C0:
            R_data = self.genData(R)
            A0_data = self.genData(A0)
        """
            B0_data = self.genData(B0)
            C0_data = self.genData(C0)
            query = MyriaQuery.submit(self.get_join_chain_program(R.name, A0.name, B0.name, C0.name))
            self.assertEqual(query.status, 'SUCCESS')

            results = MyriaRelation('public:adhoc:joinChain').to_dict()
            self.assertListOfDictsEqual(results, self.get_join_chain_expected(R_data, A0_data, B0_data, C0_data))


class IncorrectDelimiterTest(MyriaTestBase):
    def test(self):
        #TODO change URL to local file
        program = """
r = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/simple_two_col_int.txt',
                      csv(schema(x:int, y:int), delimiter=','));
store(r, r);
"""
        query = MyriaQuery.submit(program)
        self.assertEqual(query.status, 'ERROR')


class NonNullChildTest(MyriaTestBase):
    def test(self):
        program = """
r = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/simple_two_col_int.txt',
                      csv(schema(follower:int, followee:int), delimiter=' '));
store(r, jwang:global_join:smallTable);
"""
        ingest_query = MyriaQuery.submit(program)
        self.assertEqual(ingest_query.status, 'SUCCESS')

        join_json = json.loads(open('jsonQueries/nullChild_jortiz/ThreeWayLocalJoin.json').read())
        join_query = MyriaQuery.submit_plan(join_json).wait_for_completion()
        self.assertEqual(join_query.status, 'SUCCESS')


class DbDeleteTest(MyriaTestBase):
    def test(self):
        program = """
        T1 = load("s3://uwdb/sampleData/TwitterK.csv",csv(schema(a:int,b:int)));
        store(T1, public:adhoc:twitterDelete);
        """
        query = MyriaQuery.submit(program)
        self.assertEqual(query.status, 'SUCCESS')

        relation = MyriaRelation('public:adhoc:twitterDelete')
        self.assertEqual(relation.is_persisted, True)

        # delete relation and check the catalog
        relation.delete()
         self.assertRaises(MyriaError,self.connection.dataset,relation.qualified_name)


class RoundRobinAggregateTest(MyriaTestBase):
    def test(self):
        program = """
r = load('https://s3-us-west-2.amazonaws.com/bhaynestemp/simple_two_col_int.txt',
                      csv(schema(follower:int, followee:int), delimiter=' '));
store(r, jwang:global_join:smallTable);
"""
        ingest_query = MyriaQuery.submit(program)
        self.assertEqual(ingest_query.status, 'SUCCESS')

        join_json = json.loads(open('jsonQueries/nullChild_jortiz/ThreeWayLocalJoin.json').read())
        join_query = MyriaQuery.submit_plan(join_json).wait_for_completion()
        self.assertEqual(join_query.status, 'SUCCESS')


if __name__ == '__main__':
    unittest.main()
