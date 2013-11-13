package edu.washington.escience.myria.operator;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.LittleEndianDataInputStream;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;

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
  /** Data input to read data from the bin file. */
  private transient DataInput dataInput;
  /** Keeps track of the file size. */
  private long fileLength;
  /** Janino evaluator to that compiles reading. */
  private Reader evaluator;
  /** The sum of all column sizes in bytes. */
  private int tupleSize = 0;

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
  protected final TupleBatch fetchNextReady() throws DbException {
    try {
      while (fileLength > 0 && buffer.numTuples() < TupleBatch.BATCH_SIZE) {
        evaluator.read(buffer, dataInput);
        fileLength -= tupleSize;
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
      FileInputStream fStream = new FileInputStream(Objects.requireNonNull(fileName));
      fileLength = fStream.getChannel().size();
      inputStream = new BufferedInputStream(fStream);
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

    // compile reader script
    StringBuilder eb = new StringBuilder();
    for (int count = 0; count < schema.numColumns(); ++count) {
      switch (schema.getColumnType(count)) {
        case DOUBLE_TYPE:
          eb.append("buffer.putDouble(").append(count).append(", dataInput.readDouble());\n");
          tupleSize += 8;
          break;
        case FLOAT_TYPE:
          eb.append("buffer.putFloat(").append(count).append(", dataInput.readFloat());\n");
          tupleSize += 4;
          break;
        case INT_TYPE:
          eb.append("buffer.putInt(").append(count).append(", dataInput.readInt());\n");
          tupleSize += 4;
          break;
        case LONG_TYPE:
          eb.append("buffer.putLong(").append(count).append(", dataInput.readLong());\n");
          tupleSize += 8;
          break;
        default:
          throw new UnsupportedOperationException(
              "BinaryFileScan only supports reading fixed-width types from the binary file.");
      }
    }

    try {
      IScriptEvaluator se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
      evaluator = (Reader) se.createFastEvaluator(eb.toString(), Reader.class, new String[] { "buffer", "dataInput" });
    } catch (Exception e) {
      throw new DbException("Error when compiling script " + this, e);
    }
  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }
}
