package edu.washington.escience.myriad.operator.apply;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;

/**
 * Apply operator that will apply a math function on attributes as specified.
 * 
 * The returning schema will be the child operator's schema with all the attributes after being applied
 * 
 * The column type will be the same as that of the attribute specified
 * 
 */
public final class Apply extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The child operator feeding the tuples. */
  private Operator child;
  /** the field we want to apply the function on. */
  private final List<IFunctionCaller> callers;
  /** the resulting schema. */
  private final Schema schema;

  /**
   * output buffer.
   * */
  private transient TupleBatchBuffer resultBuffer;

  /**
   * Constructor.
   * 
   * @param child TupleBatch that will feed us with tuples
   * @param callers the fields we want to apply the operation on
   */
  public Apply(final Operator child, final List<IFunctionCaller> callers) {
    this.child = child;
    this.callers = callers;

    final Schema childSchema = child.getSchema();

    final ImmutableList.Builder<Type> schemaTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> schemaNames = ImmutableList.builder();

    schemaTypes.addAll(childSchema.getColumnTypes());
    schemaNames.addAll(childSchema.getColumnNames());

    for (IFunctionCaller caller : callers) {
      List<Integer> applyFields = caller.getApplyField();
      final ImmutableList.Builder<String> names = ImmutableList.builder();
      final ImmutableList.Builder<Type> typesList = ImmutableList.builder();
      for (Integer i : applyFields) {
        names.add(child.getSchema().getColumnName(i));
        typesList.add(child.getSchema().getColumnType(i));
      }
      schemaNames.add(caller.toString(names.build()));
      schemaTypes.add(caller.getResultType(typesList.build()));
    }

    schema = new Schema(schemaTypes, schemaNames);
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb = null;
    tb = child.next();
    if (tb == null) {
      return null;
    }
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < tb.numTuples(); i++) {
      // put the content from the child operator first
      for (int j = 0; j < tb.numColumns(); j++) {
        tbb.put(j, tb.getObject(j, i));
      }
      // put the result into the tbb
      for (int j = 0; j < callers.size(); j++) {
        final ImmutableList.Builder<Number> srcNums = ImmutableList.builder();
        Number value = null;
        for (Integer index : callers.get(j).getApplyField()) {
          Type applyFieldType = schema.getColumnType(index);
          if (applyFieldType == Type.INT_TYPE) {
            srcNums.add(tb.getInt(index, i));
          } else if (applyFieldType == Type.LONG_TYPE) {
            srcNums.add(tb.getLong(index, i));
          } else if (applyFieldType == Type.FLOAT_TYPE) {
            srcNums.add(tb.getFloat(index, i));
          } else if (applyFieldType == Type.DOUBLE_TYPE) {
            srcNums.add(tb.getDouble(index, i));
          }
        }
        value = callers.get(j).execute(srcNums.build());
        tbb.put(j + tb.numColumns(), value);
      }
    }
    return tbb.popAny();
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  protected void cleanup() throws DbException {
    // nothing to clean
    resultBuffer.clear();
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    if (child.eoi() || child.eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = child.nextReady()) != null) {
      for (int i = 0; i < tb.numTuples(); i++) {
        // put the content from the child operator first
        for (int j = 0; j < tb.numColumns(); j++) {
          resultBuffer.put(j, tb.getObject(j, i));
        }
        // put the result into the tbb
        for (int j = 0; j < callers.size(); j++) {
          final ImmutableList.Builder<Number> srcNums = ImmutableList.builder();
          Number value = null;
          for (Integer index : callers.get(j).getApplyField()) {
            Type applyFieldType = schema.getColumnType(index);
            if (applyFieldType == Type.INT_TYPE) {
              srcNums.add(tb.getInt(index, i));
            } else if (applyFieldType == Type.LONG_TYPE) {
              srcNums.add(tb.getLong(index, i));
            } else if (applyFieldType == Type.FLOAT_TYPE) {
              srcNums.add(tb.getFloat(index, i));
            } else if (applyFieldType == Type.DOUBLE_TYPE) {
              srcNums.add(tb.getDouble(index, i));
            }
          }
          value = callers.get(j).execute(srcNums.build());
          resultBuffer.put(j + tb.numColumns(), value);
        }
      }
      if (resultBuffer.hasFilledTB()) {
        resultBuffer.popFilled();
      }
    }
    if (child.eoi() || child.eos()) {
      return resultBuffer.popAny();
    } else {
      return resultBuffer.popFilled();
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    resultBuffer = new TupleBatchBuffer(schema);
  }

}
