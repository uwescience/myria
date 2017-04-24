package edu.washington.escience.myria.operator;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * TupleSink is a {@link RootOperator} that can be used to serialize data in a streaming fashion. It consumes
 * {@link TupleBatch}es from its child and passes them to a {@link TupleWriter}.
 *
 *
 */
public final class TupleSink extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The class that will serialize the tuple batches. */
  private final TupleWriter tupleWriter;
  private final DataSink dataSink;
  /** Whether this object has finished. */
  private boolean done = false;

  private boolean includeColumnHeader = true;

  public TupleSink(
      final Operator child,
      final TupleWriter tupleWriter,
      final DataSink dataSink,
      boolean includeColumnHeader) {
    this(child, tupleWriter, dataSink);
    this.includeColumnHeader = includeColumnHeader;
  }
  /**
   * Instantiate a new TupleSink operator, which will stream its tuples to the specified {@link TupleWriter}.
   *
   * @param child the source of tuples to be streamed.
   * @param writer the {@link TupleWriter} which will serialize the tuples.
   * @param dataSink the {@link DataSink} for the tuple destination
   */
  public TupleSink(final Operator child, final TupleWriter tupleWriter, final DataSink dataSink) {
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
      if (includeColumnHeader) {
        tupleWriter.writeColumnHeaders(getChild().getSchema().getColumnNames());
      }
    } catch (IOException e) {
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
