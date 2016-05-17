package edu.washington.escience.myria.operator;

import java.io.Serializable;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * */
public abstract class StreamingState implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** the operator that it belongs to. */
  private Operator op;

  /**
   * @return the operator that it belongs to.
   */
  protected Operator getOp() {
    return op;
  }

  /**
   * set its attached operator for inferring schema.
   *
   * @param op the operator that it's attached on.
   */
  public void setAttachedOperator(final Operator op) {
    this.op = op;
  }

  /**
   * initialization, as what we have in Operator.
   *
   * @param execEnvVars environment variables.
   */
  public abstract void init(final ImmutableMap<String, Object> execEnvVars);

  /**
   * cleanup, as what we have in Operator.
   */
  public abstract void cleanup();

  /**
   * takes a TB, updates its internel states, and return a TB if applicable.
   *
   * @param tb the input tuple batch.
   * @return the generated tuple batch.
   * */
  public abstract TupleBatch update(TupleBatch tb);

  /**
   * @return its output schema.
   */
  public abstract Schema getSchema();

  /**
   *
   * @return its internal state as tuple batch buffer.
   */
  public abstract List<TupleBatch> exportState();

  /**
   * @return the number of tuples of state.
   */
  public abstract int numTuples();

  /**
   * @return a new instance of StreamingState with all the constructor arguments copied.
   * */
  public abstract StreamingState newInstanceFromMyself();
}
