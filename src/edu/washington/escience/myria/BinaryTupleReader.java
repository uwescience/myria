package edu.washington.escience.myria;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 *
 */
public class BinaryTupleReader implements TupleReader {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The schema for the relation stored in this file. */
  private final Schema schema;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Indicates the endianess of the bin file to read. */
  private final boolean isLittleEndian;
  /** Data input to read data from the bin file. */
  private transient DataInput dataInput;

  /**
   * Construct a new BinaryTupleReader object that reads the given binary file and creates tuples from the file data
   * that has the given schema. The default endianess is big endian.
   *
   * @param schema The tuple schema to be used for creating tuple from the binary file's data.
   */
  public BinaryTupleReader(final Schema schema) {
    this(schema, false);
  }

  /**
   * Construct a new BinaryTupleReader object that reads the given binary file and create tuples from the file data that
   * has the given schema. The endianess of the binary file is indicated by the isLittleEndian flag.
   *
   * @param schema The tuple schema to be used for creating tuple from the binary file's data.
   * @param isLittleEndian The flag that indicates the endianess of the binary file.
   */
  public BinaryTupleReader(final Schema schema, final boolean isLittleEndian) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.isLittleEndian = isLittleEndian;
  }

  @Override
  public void open(final InputStream stream) throws IOException, DbException {
    buffer = new TupleBatchBuffer(schema);
    InputStream inputStream = new BufferedInputStream(stream);

    if (isLittleEndian) {
      dataInput = new LittleEndianDataInputStream(inputStream);
    } else {
      dataInput = new DataInputStream(inputStream);
    }
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public TupleBatch readTuples() throws IOException, DbException {
    boolean building = false;
    try {

      while (buffer.numTuples() < buffer.getBatchSize()) {
        for (int count = 0; count < schema.numColumns(); ++count) {
          switch (schema.getColumnType(count)) {
            case BOOLEAN_TYPE:
              buffer.putBoolean(count, dataInput.readBoolean());
              break;
            case DOUBLE_TYPE:
              buffer.putDouble(count, dataInput.readDouble());
              break;
            case FLOAT_TYPE:
              buffer.putFloat(count, dataInput.readFloat());
              break;
            case INT_TYPE:
              buffer.putInt(count, dataInput.readInt());
              break;
            case LONG_TYPE:
              buffer.putLong(count, dataInput.readLong());
              break;
            case STRING_TYPE:
              buffer.putString(count, dataInput.readUTF());
              break;
            default:
              throw new UnsupportedOperationException(
                  "BinaryTupleReader does not support the type " + schema.getColumnType(count));
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
  public void close() throws IOException {
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }
}
