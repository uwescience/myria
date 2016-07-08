package edu.washington.escience.myria.operator;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Reads data from binary file. This class is written base on the code from FileScan.java
 *
 * @author leelee
 *
 */
public class BinaryFileScan extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The schema for the relation stored in this file. */
  private final Schema schema;
  /** The source of the input data. */
  private final DataSource source;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Indicates the endianess of the bin file to read. */
  private final boolean isLittleEndian;
  /** Data input to read data from the bin file. */
  private transient DataInput dataInput;

  /**
   * Construct a new BinaryFileScan object that reads the given binary file and create tuples from the file data that
   * has the given schema. The endianess of the binary file is indicated by the isLittleEndian flag.
   *
   * @param schema The tuple schema to be used for creating tuple from the binary file's data.
   * @param source The source of the binary input data.
   * @param isLittleEndian The flag that indicates the endianess of the binary file.
   */
  public BinaryFileScan(
      final Schema schema, final DataSource source, final boolean isLittleEndian) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.source = Objects.requireNonNull(source, "source");
    this.isLittleEndian = isLittleEndian;
  }

  /**
   * Construct a new BinaryFileScan object that reads the given binary file and creates tuples from the file data that
   * has the given schema. The default endianess is big endian.
   *
   * @param schema The tuple schema to be used for creating tuple from the binary file's data.
   * @param source The source of the binary input data.
   */
  public BinaryFileScan(final Schema schema, final DataSource source) {
    this(schema, source, false);
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    boolean building = false;
    try {
      while (buffer.numTuples() < TupleBatch.BATCH_SIZE) {
        for (int count = 0; count < schema.numColumns(); ++count) {
          switch (schema.getColumnType(count)) {
            case DOUBLE_TYPE:
              buffer.putDouble(count, dataInput.readDouble());
              break;
            case FLOAT_TYPE:
              float readFloat = dataInput.readFloat();
              buffer.putFloat(count, readFloat);
              break;
            case INT_TYPE:
              buffer.putInt(count, dataInput.readInt());
              break;
            case LONG_TYPE:
              long readLong = dataInput.readLong();
              buffer.putLong(count, readLong);
              break;
            default:
              throw new UnsupportedOperationException(
                  "BinaryFileScan only support reading fixed width type from the binary file.");
          }
          building = true;
        }
        building = false;
      }
    } catch (EOFException e) {
      if (!building) {
        /* Do nothing -- we got an exception because the data ran out at the right place. */
        ;
      } else {
        throw new DbException("Ran out of binary data in the middle of a row", e);
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
    TupleBatch tb = buffer.popAny();
    return tb;
  }

  @Override
  protected final void cleanup() throws DbException {
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    InputStream inputStream;
    try {
      inputStream = new BufferedInputStream(source.getInputStream());
    } catch (FileNotFoundException e) {
      throw new DbException(e);
    } catch (IOException e) {
      throw new DbException(e);
    }

    if (isLittleEndian) {
      dataInput = new LittleEndianDataInputStream(inputStream);
    } else {
      dataInput = new DataInputStream(inputStream);
    }
  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }
}
