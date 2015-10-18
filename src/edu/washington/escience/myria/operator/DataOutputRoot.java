package edu.washington.escience.myria.operator;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * RemoteDataOutput is ...
 */
public final class DataOutputRoot extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final DataSink dataSink;
  private final TupleWriter tupleWriter;

  private boolean done = false;

  public DataOutputRoot(final Operator child, final TupleWriter tupleWriter, final DataSink dataSink) {
    super(child);
    this.dataSink = dataSink;
    this.tupleWriter = tupleWriter;
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
    done = true;
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
      tupleWriter.open(dataSink.getOutputStream());
      tupleWriter.writeColumnHeaders(getChild().getSchema().getColumnNames());
    } catch (IOException | SecurityException | IllegalArgumentException e) {
      throw new DbException(e);
    }
  }

  @Override
  protected void cleanup() throws IOException {
    if (!done) {
      tupleWriter.error();
    }
  }
}
