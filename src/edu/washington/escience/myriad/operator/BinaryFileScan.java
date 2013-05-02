package edu.washington.escience.myriad.operator;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;

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
  /** The filename of the input. */
  private final String fileName;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Indicates the endianess of the bin file to read. */
  private final boolean isLittleEndian;
  /** FileInputStream for the bin file. */
  private FileInputStream fStream;
  /** Data input to read data from the bin file. */
  private DataInput dataInput;
  /** FileChannel for fStream. */
  private FileChannel fc;
  /** Keeps track of the file size. */
  private long fileLength;

  /**
   * Construct a new BinaryFileScan object that reads the given binary file and create tuples from the file data that
   * has the given schema. The endianess of the binary file is indicated by the isLittleEndian flag.
   * 
   * @param schema The tuple schema to be used for creating tuple from the binary file's data.
   * @param fileName The binary file name.
   * @param isLittleEndian The flag that indicates the endianess of the binary file.
   */
  public BinaryFileScan(final Schema schema, final String fileName, final boolean isLittleEndian) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(fileName);
    this.schema = schema;
    this.fileName = fileName;
    this.isLittleEndian = isLittleEndian;
  }

  /**
   * Construct a new BinaryFileScan object that reads the given binary file and creates tuples from the file data that
   * has the given schema. The default endianess is big endian.
   * 
   * @param schema The tuple schema to be used for creating tuple from the binary file's data.
   * @param fileName The binary file name.
   */
  public BinaryFileScan(final Schema schema, final String fileName) {
    this(schema, fileName, false);
  }

  @Override
  protected final TupleBatch fetchNext() throws DbException {
    try {
      while (fileLength > 0 && buffer.numTuples() < TupleBatch.BATCH_SIZE) {
        for (int count = 0; count < schema.numColumns(); ++count) {
          switch (schema.getColumnType(count)) {
            case DOUBLE_TYPE:
              buffer.put(count, dataInput.readDouble());
              fileLength -= 8;
              break;
            case FLOAT_TYPE:
              float readFloat = dataInput.readFloat();
              buffer.put(count, readFloat);
              fileLength -= 4;
              break;
            case INT_TYPE:
              buffer.put(count, dataInput.readInt());
              fileLength -= 4;
              break;
            case LONG_TYPE:
              long readLong = dataInput.readLong();
              buffer.put(count, readLong);
              fileLength -= 8;
              break;
            default:
              throw new UnsupportedOperationException(
                  "BinaryFileScan only support reading fixed width type from the binary file.");
          }
        }
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
  protected final TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    if (fileName != null) {
      fStream = null;
      try {
        fStream = new FileInputStream(fileName);
        fc = fStream.getChannel();
        fileLength = fc.size();
      } catch (FileNotFoundException e) {
        throw new DbException(e);
      } catch (IOException e) {
        throw new DbException(e);
      }
      if (isLittleEndian) {
        dataInput = new LittleEndianDataInputStream(fStream);
      } else {
        dataInput = new DataInputStream(fStream);
      }
    }
  }
}
