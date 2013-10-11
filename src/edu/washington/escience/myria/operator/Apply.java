package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;

/**
 * Generic apply operator.
 */
public class Apply extends UnaryOperator {

  /**
   * List of expressions that will be used to create the output.
   */
  private final List<Expression> expressions;

  /**
   * Buffers the output tuples.
   */
  private TupleBatchBuffer resultBuffer;

  /**
   * The output schema.
   */
  private Schema schema;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param expressions expression that created the output
   */
  public Apply(final Operator child, final List<Expression> expressions) {
    super(child);
    this.expressions = expressions;
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch tb = null;
    if (getChild().eoi() || getChild().eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = getChild().nextReady()) != null) {
      int columnIdx = 0;
      for (Expression expr : expressions) {
        for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
          Object result = expr.eval(tb, rowIdx);
          resultBuffer.put(columnIdx, result);
        }
        columnIdx++;
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
    resultBuffer = new TupleBatchBuffer(getSchema());

    for (Expression expr : expressions) {
      expr.compile(getSchema());
    }
  }

  @Override
  public Schema getSchema() {
    // TODO: use column names
    if (schema == null) {
      List<Type> types = new ArrayList<Type>();

      for (Expression expr : expressions) {
        types.add(expr.getOutputType(getChild().getSchema()));
      }
      schema = new Schema(types);
    }

    return schema;
  }
}
