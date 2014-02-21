package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.JsonTupleWriter;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.api.DatasetFormat;

/**
 * DataOutput is a {@link RootOperator} that can be used to serialize data in a streaming fashion. It consumes
 * {@link TupleBatch}es from its child and passes them to a {@link TupleWriter}.
 * 
 * @author dhalperi
 * 
 */
public final class DataOutput extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The class that will serialize the tuple batches. */
  private TupleWriter tupleWriter;
  /** Output stream the tuples write to. */
  private PipedOutputStream writerOutput;
  /** Output format. */
  private final DatasetFormat format;

  /**
   * Instantiate a new DataOutput operator, which will stream its tuples to the specified {@link TupleWriter}.
   * 
   * @param child the source of tuples to be streamed.
   * @param format output format.
   */
  public DataOutput(final Operator child, final DatasetFormat format) {
    super(child);
    Preconditions.checkNotNull(format);
    this.format = format;
  }

  @Override
  protected void childEOI() throws DbException {
    /* Do nothing. */
  }

  @Override
  protected void childEOS() throws DbException {

  }

  @Override
  protected void cleanup() throws DbException {
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

  /** @param sink the pipe sink to which the output can be read. */
  public void connectSink(final PipedInputStream sink) {
    try (PipedOutputStream writerOutput = new PipedOutputStream()) {

      /* Set up the TupleWriter and the Response MediaType based on the format choices. */
      TupleWriter writer = null;
      switch (format) {
        case CSV:
          writer = new CsvTupleWriter(writerOutput);
          break;
        case TSV:
          writer = new CsvTupleWriter(DatasetFormat.TSV.getColumnDelimiter(), writerOutput);
          break;
        case JSON:
          writer = new JsonTupleWriter(writerOutput);
          break;
        default:
          throw new IllegalStateException("format should have been validated by now, and yet we got here");
      }
      tupleWriter = writer;
      this.writerOutput = writerOutput;
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }

    try {
      writerOutput.connect(sink);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
