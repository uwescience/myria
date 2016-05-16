package edu.washington.escience.myria.operator;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Read a SeaFlow EVT/OPP file. See the formats in https://github.com/fribalet/flowPhyto/blob/master/R/Globals.R
 *
 * This operator implements file format version 3.
 */
public class SeaFlowFileScan extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The data input file. */
  private transient DataInput input;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;

  /** The group number file name. */
  private final DataSource source;
  /** Which record the reader is currently on. */
  private int lineNumber;
  /** The expected number of rows in the file. */
  private int numRows;
  /** The magic number at the end of each line (but the last) in a SeaFlow file. */
  private static final int EOL = 10;

  /** Schema for all SeaFlow event files. */
  private static final Schema OPP_SCHEMA =
      new Schema(
          ImmutableList.of(
              Type.INT_TYPE, // time
              Type.INT_TYPE, // pulse_width
              Type.INT_TYPE, // D1
              Type.INT_TYPE, // D2
              Type.INT_TYPE, // fsc_small
              Type.INT_TYPE, // fsc_perp
              Type.INT_TYPE, // fsc_big
              Type.INT_TYPE, // pe
              Type.INT_TYPE, // chl_small
              Type.INT_TYPE // chl_big
              ),
          ImmutableList.of(
              "time",
              "pulse_width",
              "D1",
              "D2",
              "fsc_small",
              "fsc_perp",
              "fsc_big",
              "pe",
              "chl_small",
              "chl_big"));
  /** The number of columns in the schema of a SeaFlow EVT/OPP file. */
  private static final int NUM_COLUMNS = OPP_SCHEMA.numColumns();
  /** The number of bytes in one row of a SeaFlow EVT/OPP file. */
  private static final int COLUMN_SIZE = OPP_SCHEMA.numColumns() * 2 + 4;

  /**
   * Construct a SeaFlowFileScan reading the specified local file.
   *
   * @param filename the file to be read.
   */
  public SeaFlowFileScan(final String filename) {
    this(new FileSource(filename));
  }

  /**
   * Construct a SeaFlowFileScan reading the data in the specified data source.
   *
   * @param source contains the data to be read.
   */
  public SeaFlowFileScan(final DataSource source) {
    this.source = Objects.requireNonNull(source);
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    while ((lineNumber < numRows) && (buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      try {
        /*
         * Every line but the last, including the header, is terminated with a 32-bit unsigned int with the value 10. We
         * read the EOL for the header/previous line before the current line to simplify the EOF checking.
         */
        Preconditions.checkState(input.readInt() == EOL);
        for (int col = 0; col < NUM_COLUMNS; ++col) {
          buffer.putInt(col, input.readUnsignedShort());
        }
      } catch (final IOException e) {
        throw new DbException("Exception in line " + lineNumber, e);
      }
      lineNumber++;
    }

    if (lineNumber == numRows) {
      /* Check for an EOF, and error if not. */
      boolean flag = false;
      try {
        input.readByte();
      } catch (EOFException e) {
        flag = true;
      } catch (IOException e) {
        throw new DbException("Error when verifying EOF after line " + lineNumber, e);
      }
      Preconditions.checkState(
          flag, "Was able to read another byte after %s rows, expected EOFException", lineNumber);
    }
    return buffer.popAny();
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());

    try {
      input = new LittleEndianDataInputStream(new BufferedInputStream(source.getInputStream()));
      numRows = input.readInt(); /* number of rows */
      /* If the source is a FileSource, we can actually check its length. */
      if (source instanceof FileSource) {
        long length = Files.size(Paths.get(((FileSource) source).getFilename()));
        long expectedSize = 4 + numRows * COLUMN_SIZE;
        Preconditions.checkArgument(
            length == expectedSize,
            "Given %s rows, expected a file of length %s, not %s",
            numRows,
            expectedSize,
            length);
      }
    } catch (IOException e) {
      throw new DbException(e);
    }

    lineNumber = 0;
  }

  @Override
  protected final void cleanup() throws DbException {
    buffer.clear();
  }

  @Override
  protected Schema generateSchema() {
    return OPP_SCHEMA;
  }
}
