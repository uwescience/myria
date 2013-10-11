package edu.washington.escience.myria.operator;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleWriter;

/**
 * DataOutput is a {@link RootOperator} that can be used to serialize data in a streaming fashion. It consumes
 * {@link TupleBatch}es from its child and passes them to a {@link TupleWriter}.
 * 
 * @author dhalperi
 * 
 */
public class DataOutput extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The class that will serialize the tuple batches. */
  private final TupleWriter tupleWriter;

  /**
   * Instantiate a new DataOutput operator, which will stream its tuples to the specified {@link TupleWriter}.
   * 
   * @param child the source of tuples to be streamed.
   * @param writer the {@link TupleWriter} which will serialize the tuples.
   */
  public DataOutput(final Operator child, final TupleWriter writer) {
    super(child);
    tupleWriter = writer;
  }

  @Override
  protected void childEOI() throws DbException {
    /* Do nothing. */
  }

  @Override
  protected void childEOS() throws DbException {
    try {
      tupleWriter.done();
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {
    try {
      tupleWriter.writeTuples(tuples);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    try {
      tupleWriter.writeColumnHeaders(getChild().getSchema().getColumnNames());
    } catch (IOException e) {
      throw new DbException(e);
    }
  }
}
