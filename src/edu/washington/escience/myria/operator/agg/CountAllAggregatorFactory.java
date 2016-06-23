package edu.washington.escience.myria.operator.agg;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;

/**
 * A factory for the CountAll aggregator.
 */
public final class CountAllAggregatorFactory implements AggregatorFactory {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Instantiate a CountAllFactory. */
  public CountAllAggregatorFactory() {
    /** Nothing needed here. */
  }

  @Override
  public Aggregator get(final Schema inputSchema) throws DbException {
    return new CountAllAggregator();
  }

  @Override
  public Aggregator get(final Schema inputSchema, final PythonFunctionRegistrar pyFuncReg) throws DbException {
    return get(inputSchema);
  }
}
