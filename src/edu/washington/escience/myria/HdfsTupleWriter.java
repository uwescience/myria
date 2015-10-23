package edu.washington.escience.myria;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;

import edu.washington.escience.myria.storage.ReadableTable;

/**
 * HdfsTupleWriter is a {@link TupleWriter} that writes tuples to HDFS.
 */
public class HdfsTupleWriter implements TupleWriter {

  /** Required for Java serialization. */
  static final long serialVersionUID = 1L;

  /** The {@link OutputStream} to which we write the data. **/
  private FSDataOutputStream outStream;

  /**
   * Constructs a {@link HdfsTupleWriter}.
   */
  public HdfsTupleWriter() {
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
    /* Write each row to the output stream */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      String row = "";
      for (int j = 0; j < tuples.numColumns(); ++j) {
        row += tuples.getObject(j, i).toString();
        row += j == tuples.numColumns() - 1 ? '\n' : ',';
      }
      outStream.writeUTF(row);
    }
  }

  @Override
  public void done() throws IOException {
    outStream.close();
  }

  @Override
  public void error() throws IOException {
    try {
      outStream.writeUTF("There was an error writing the data");
    } finally {
      outStream.close();
    }
  }

}
