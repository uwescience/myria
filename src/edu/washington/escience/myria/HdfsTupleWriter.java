/**
 *
 */
package edu.washington.escience.myria;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.storage.ReadableTable;

/**
 * 
 */
public class HdfsTupleWriter implements TupleWriter {

  /** Required for Java serialization. */
  static final long serialVersionUID = 1L;

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTupleWriter.class);

  private static FSDataOutputStream outStream;

  public HdfsTupleWriter() {
  }

  public HdfsTupleWriter(final OutputStream out) {
    outStream = (FSDataOutputStream) out;
  }

  @Override
  public void open(final OutputStream stream) {
    outStream = (FSDataOutputStream) stream;
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {

    List<Type> columnTypes = tuples.getSchema().getColumnTypes();
    for (int i = 0; i < tuples.numTuples(); ++i) {
      for (int j = 0; j < tuples.numColumns(); ++j) {
        switch (columnTypes.get(j)) {
          case BOOLEAN_TYPE:
            outStream.writeBoolean(tuples.getBoolean(j, i));
            break;
          case DOUBLE_TYPE:
            outStream.writeDouble(tuples.getDouble(j, i));
            break;
          case FLOAT_TYPE:
            outStream.writeFloat(tuples.getFloat(j, i));
            break;
          case INT_TYPE:
            outStream.writeInt(tuples.getInt(j, i));
            break;
          case LONG_TYPE:
            outStream.writeLong(tuples.getLong(j, i));
            break;
          case DATETIME_TYPE: // outStream.writeUTF(tuples.getDateTime(j,i));
            break;
          case STRING_TYPE:
            outStream.writeChars(tuples.getString(j, i));
            break;
          default:
            break;
        }
      }
    }

  }

  @Override
  public void done() throws IOException {
    outStream.close();
  }

  @Override
  public void error() throws IOException {
  }

}
