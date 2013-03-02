package edu.washington.escience.myriad.operator.apply;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;

/**
 * Apply operator that will apply a math function on attributes as specified.
 * 
 * The returning schema will be the child operator's schema with all the
 * attributes after being applied
 * 
 * The column type will be the same as that of the attribute specified
 * 
 */
public class Apply extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child;
  /** the field we want to apply the function on */
  private final int[] applyField;
  private final IFunction[] functions;
  private final Schema schema;

  /**
   * Constructor
   * 
   * @param child
   *          TupleBatch that will feed us with tuples
   * @param applyField
   *          the fields we want to apply the operation on
   */
  public Apply(final Operator child, final int[] applyField,
      final IFunction[] functions) {
    this.child = child;
    this.applyField = applyField;
    this.functions = functions;

    final Schema childSchema = child.getSchema();

    final ImmutableList.Builder<Type> schemaTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> schemaNames = ImmutableList.builder();

    schemaTypes.addAll(childSchema.getTypes());
    schemaNames.addAll(childSchema.getFieldNames());

    for (int i = 0; i < applyField.length; i++) {
      // for of the fields add a type and a name to it
      String header = functions[i].toString() + "("
          + childSchema.getFieldName(applyField[i]) + ")";
      schemaNames.add(header);
      schemaTypes.add(functions[i].getResultType(childSchema
          .getFieldType(applyField[i])));
    }
    schema = new Schema(schemaTypes, schemaNames);
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
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
      for (int j = 0; j < functions.length; j++) {
        Type applyFieldType = tb.getSchema().getFieldType(applyField[j]);
        Number value = null;
        if (applyFieldType == Type.INT_TYPE) {
          value = functions[j].execute(tb.getInt(applyField[j], i));
        } else if (applyFieldType == Type.LONG_TYPE) {
          value = functions[j].execute(tb.getLong(applyField[j], i));
        } else if (applyFieldType == Type.DOUBLE_TYPE) {
          value = functions[j].execute(tb.getDouble(applyField[j], i));
        }
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
  protected void init() throws DbException {
    // nothing to init
  }

  @Override
  protected void cleanup() throws DbException {
    // nothing to clean
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void setChildren(Operator[] children) {
    child = children[0];
  }

}
