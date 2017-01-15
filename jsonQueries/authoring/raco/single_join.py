#!python

import collections
import json
import types

import raco
import raco.expression as expr
# Algebra contains logical operators like Scan, Shuffle, Join
import raco.algebra as logical
# MyriaLang contains Myria physical operators like MyriaScan, MyriaShuffleProducer, MyriaSymmetricHashJoin
import raco.myrialang as myria

def json_pretty_print(dictionary):
    """a function to pretty-print a JSON dictionary.
From http://docs.python.org/2/library/json.html"""
    return json.dumps(dictionary, sort_keys=True,
            indent=2, separators=(',', ': '))

def is_atom(obj):
    """Return True if the object is an atom, e.g., a string/int/etc. and not a list/generator/etc."""
    return obj is not None and (not isinstance(obj, collections.Iterable) \
                                 or isinstance(obj, types.StringTypes))

def ensure_list(obj):
    if obj is None:
        return []
    if is_atom(obj):
        return [obj]
    return obj

def convert_col_refs(refs):
    """Given a list of column representations, produce a list of raco AttributeRefs"""
    def col_to_ref(i):
        """given a reasonable representation of a column, e.g., an int [0], 
        a string ['x'], or a raco AttributeRef, produce an AttributeRef."""
        if issubclass(i.__class__, expr.AttributeRef):
            return i
        elif type(i) is int:
            return expr.UnnamedAttributeRef(i)
        elif type(i) is str:
            return expr.NamedAttributeRef(i)

    # Gracefully handle the case when user supplies an atom instead of a list
    refs = ensure_list(refs)

    return [col_to_ref(ref) for ref in refs]

def equijoin_condition(left_cols, right_cols):
    """Given two lists of column representations (int positions, string names,
    or raco AttributeRefs), produce the raco join condition. If the lists are
    empty, the join condition will be expr.TAUTOLOGY.
    
    Note that all references must refer to the OUTPUT scheme. So R(x,y),R(y,z)
    must imply [1],[2] as the two arguments."""
    # Gracefully convert atoms / None to singleton list / empty list
    left_cols = ensure_list(left_cols)
    right_cols = ensure_list(right_cols)

    # Sanity check
    assert len(left_cols) == len(right_cols)

    # No conditions => Tautology
    if len(left_cols) == 0:
        return expr.TAUTOLOGY

    left_refs = convert_col_refs(left_cols)
    right_refs = convert_col_refs(right_cols)
    conds = [expr.EQ(left, right) for (left, right) in zip(left_refs, right_refs)]
    if len(conds) == 1:
        return conds[0]
    def make_and(l,r):
        return expr.AND(l, r)
    return reduce(make_and, conds)

def shuffle_by_field(operator, fields, is_logical=True):
    field_refs = convert_col_refs(fields)
    field_pos = [ref.get_position(operator.scheme()) for ref in field_refs]
    if is_logical:
        return logical.Shuffle(operator, field_pos)
    return myria.MyriaShuffleConsumer(myria.MyriaShuffleProducer(operator, field_pos))

def from_datalog():
    query = "A(x,z) :- R(x,y), R(y,z)"

    # Create a compiler object and parse the Datalog query.
    compiler = raco.RACompiler()
    compiler.fromDatalog(query)

    # Later optimization will actually mutate the query plan, so save the plan
    # out as a string now
    cached_logicalplan = str(compiler.logicalplan)

    # Optimize the query, including producing a physical plan
    compiler.optimize(target=myria.MyriaAlgebra, eliminate_common_subexpressions=False)

    # Compile it to JSON
    myria_plan = myria.compile_to_json(query, cached_logicalplan, compiler.physicalplan)

    return myria_plan

def from_logical_plan():
    # Construct the logical plan manually
    scanL = logical.Scan('R', raco.scheme.Scheme([('x', None), ('y1', None)]))
    # .. right
    scanR = logical.Scan('R', raco.scheme.Scheme([('y2', None), ('z', None)]))
    # Join
    join = logical.Join(equijoin_condition(['y1'], ['y2']), scanL, scanR)
    # Project
    project = logical.Project(convert_col_refs(['x', 'z']), join)
    # Plan
    logicalplan = [('A', project)]
    # Later optimization will actually mutate the query plan, so save the plan
    # out as a string now
    cached_logicalplan = str(logicalplan)

    # Create a new compiler object and shim in the logical plan
    compiler = raco.RACompiler()
    compiler.logicalplan = logicalplan

    # Optimize the query, including producing a physical plan
    compiler.optimize(target=myria.MyriaAlgebra, eliminate_common_subexpressions=False)

    # Compile it to JSON
    myria_plan = myria.compile_to_json("no query--direct logical plan",
            cached_logicalplan, compiler.physicalplan)
    
    return myria_plan

def from_logical_parallel_plan():
    # Construct the logical plan manually
    scanL = logical.Scan('R', raco.scheme.Scheme([('x', None), ('y1', None)]))
    shuffleL = shuffle_by_field(scanL, 'y1')
    # .. right
    scanR = logical.Scan('R', raco.scheme.Scheme([('y2', None), ('z', None)]))
    shuffleR = shuffle_by_field(scanR, 'y2')
    # Join
    join = logical.Join(equijoin_condition(['y1'], ['y2']), shuffleL, shuffleR)
    # Project
    project = logical.Project(convert_col_refs(['x', 'z']), join)
    # Plan
    logicalplan = [('A', project)]
    # Later optimization will actually mutate the query plan, so save the plan
    # out as a string now
    cached_logicalplan = str(logicalplan)

    # Create a new compiler object and shim in the logical plan
    compiler = raco.RACompiler()
    compiler.logicalplan = logicalplan

    # Optimize the query, including producing a physical plan
    compiler.optimize(target=myria.MyriaAlgebra, eliminate_common_subexpressions=False)

    # Compile it to JSON
    myria_plan = myria.compile_to_json("no query--direct logical plan",
            cached_logicalplan, compiler.physicalplan)
    
    return myria_plan

def from_myria_physical_plan():
    # Construct the Myria physical plan manually
    scanL = myria.MyriaScan('R', raco.scheme.Scheme([('x', None), ('y', None)]))
    shuffleL = shuffle_by_field(scanL, 'y', False)
    # .. right
    scanR = myria.MyriaScan('R', raco.scheme.Scheme([('y', None), ('z', None)]))
    shuffleR = shuffle_by_field(scanR, 'y', False)
    # Join
    join = myria.MyriaSymmetricHashJoin(equijoin_condition([1], [2]),
            shuffleL, shuffleR, # left, right
            convert_col_refs([0, 3])) # optional project list; all cols if None
    # Plan
    physicalplan = [('A', join)]
    
    myria_plan = myria.compile_to_json("no query--direct physical plan", \
            "no logical plan--direct physical plan", \
            physicalplan)

    return myria_plan

if __name__ == "__main__":
    print json.dumps(from_datalog())
    print json.dumps(from_logical_plan())
    print json.dumps(from_logical_parallel_plan())
    print json.dumps(from_myria_physical_plan())
