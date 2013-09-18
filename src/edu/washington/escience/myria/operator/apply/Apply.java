package edu.washington.escience.myria.operator.apply;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;

/**
 * Apply operator that will apply a math function on attributes as specified.
 * 
 * The returning schema will be the child operator's schema with all the attributes after being applied
 * 
 * The column type will be the same as that of the attribute specified
 * 
 */
public final class Apply extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** the field we want to apply the function on. */
  private final List<IFunctionCaller> callers;
  /** the resulting schema. */
  private Schema schema;

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
    super(child);
    this.callers = callers;
  }

  @Override
  protected void cleanup() throws DbException {
    // nothing to clean
    resultBuffer.clear();
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = getChild().nextReady()) != null) {
      for (int i = 0; i < tb.numTuples(); i++) {
        // put the content from the child operator first
        for (int j = 0; j < tb.numColumns(); j++) {
          resultBuffer.put(j, tb.getDataColumns().get(j), i);
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
    if (getChild().eoi() || getChild().eos()) {
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
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Schema childSchema = getChild().getSchema();

    final ImmutableList.Builder<Type> schemaTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> schemaNames = ImmutableList.builder();

    schemaTypes.addAll(childSchema.getColumnTypes());
    schemaNames.addAll(childSchema.getColumnNames());

    for (IFunctionCaller caller : callers) {
      List<Integer> applyFields = caller.getApplyField();
      final ImmutableList.Builder<String> names = ImmutableList.builder();
      final ImmutableList.Builder<Type> typesList = ImmutableList.builder();
      for (Integer i : applyFields) {
        names.add(childSchema.getColumnName(i));
        typesList.add(childSchema.getColumnType(i));
      }
      schemaNames.add(caller.toString(names.build()));
      schemaTypes.add(caller.getResultType(typesList.build()));
    }

    schema = new Schema(schemaTypes, schemaNames);
    resultBuffer = new TupleBatchBuffer(schema);
  }

}
