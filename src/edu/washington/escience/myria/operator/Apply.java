package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.GenericExpression;

/**
 * Generic apply operator.
 */
public class Apply extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * List of expressions that will be used to create the output.
   */
  private ImmutableList<GenericExpression> genericExpressions;

  /**
   * Buffers the output tuples.
   */
  private TupleBatchBuffer resultBuffer;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param genericExpressions expression that created the output
   */
  public Apply(final Operator child, final List<GenericExpression> genericExpressions) {
    super(child);
    if (genericExpressions != null) {
      setExpressions(genericExpressions);
    }
  }

  /**
   * Set the expressions for each column.
   * 
   * @param genericExpressions the expressions
   */
  private void setExpressions(final List<GenericExpression> genericExpressions) {
    this.genericExpressions = ImmutableList.copyOf(genericExpressions);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch tb = null;
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = getChild().nextReady()) != null) {
      for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
        int columnIdx = 0;
        for (GenericExpression expr : genericExpressions) {
          expr.evalAndPut(tb, rowIdx, resultBuffer, columnIdx);
          columnIdx++;
        }
      }
      if (resultBuffer.hasFilledTB()) {
        return resultBuffer.popFilled();
      }
    }
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    } else {
      return resultBuffer.popFilled();
    }
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkNotNull(genericExpressions);

    resultBuffer = new TupleBatchBuffer(getSchema());

    Schema inputSchema = getChild().getSchema();

    for (GenericExpression expr : genericExpressions) {

      expr.setSchema(inputSchema);
      if (expr.needsCompiling()) {
        expr.compile();
      }
    }
  }

  @Override
  public Schema generateSchema() {
    if (genericExpressions == null) {
      return null;
    }
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema childSchema = child.getSchema();
    if (childSchema == null) {
      return null;
    }

    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : genericExpressions) {
      expr.setSchema(childSchema);
      typesBuilder.add(expr.getOutputType());
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
