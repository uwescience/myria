#!/usr/bin/env python

import unittest
from myria import MyriaConnection, MyriaRelation


class MyriaTestBase(unittest.TestCase):
    def setUp(self):
        self.connection = MyriaConnection(hostname='localhost', port=8753)


class DoWhileTest(MyriaTestBase):
    def test(self):
        program = """
x = [0 as val, 1 as exp];
do
  x = [from x emit val+1 as val, 2*exp as exp];
while [from x emit max(val) < 5];
store(x, powersOfTwo);
"""
        self.connection.execute_program(program=program)
        relation = MyriaRelation(
            'public:adhoc:powersOfTwo', connection=self.connection)
        results = relation.to_dict()
        expected = [{'val': 32, 'exp': 5}]
        self.assertEqual(results, expected)


if __name__ == '__main__':
    unittest.main()
