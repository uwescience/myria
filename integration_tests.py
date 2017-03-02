#!/usr/bin/env python

import unittest
from myria import MyriaConnection, MyriaRelation


class MyriaTestBase(unittest.TestCase):
    def setUp(self):
        connection = MyriaConnection(hostname='localhost', port=8753, execution_url="http://127.0.0.1:8080")
        MyriaRelation.DefaultConnection = connection
        self.connection = connection

    def execute(self, program):
        self.connection.execute_program(program=program)


class DoWhileTest(MyriaTestBase):
    def test(self):
        program = """
x = [0 as exp, 1 as val];
do
  x = [from x emit exp+1 as exp, 2*val as val];
while [from x emit max(exp) < 5];
store(x, powersOfTwo);
"""
        self.execute(program)
        relation = MyriaRelation('public:adhoc:powersOfTwo')
        results = relation.to_dict()
        expected = [{'val': 32, 'exp': 5}]
        assertEqual(results, expected)


if __name__ == '__main__':
    unittest.main()
