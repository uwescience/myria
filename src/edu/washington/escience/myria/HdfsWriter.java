/**
 *
 */
package edu.washington.escience.myria;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;

import edu.washington.escience.myria.storage.ReadableTable;

/**
 * 
 */
public class HdfsWriter implements TupleWriter {

  /** Required for Java serialization. */
  static final long serialVersionUID = 1L;

  FSDataOutputStream outStream;

  public HdfsWriter(final OutputStream out) {
    outStream = (FSDataOutputStream) out;
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    for (int i = 0; i < tuples.numTuples(); ++i) {
      // not sure
      outStream.writeBytes(tuples.toString());
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
