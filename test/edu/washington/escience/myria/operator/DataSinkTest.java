/**
 *
 */
package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

import org.junit.Test;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.ByteArraySource;
import edu.washington.escience.myria.io.ByteSink;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 *
 */
public class DataSinkTest {

  @Test
  public void testDataSink() throws Exception {
    /* Read a CSV and construct the query */
    String dataSrc = "x,y\r\n1,2\r\n3,4\r\n5,6\r\n7,8\r\n";
    byte[] srcBytes = dataSrc.getBytes(Charset.forName("UTF-8"));
    Schema relationSchema = Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE);
    DataSource byteSource = new ByteArraySource(srcBytes);
    FileScan fileScan = new FileScan(byteSource, relationSchema, ',', null, null, 1);
    ByteSink byteSink = new ByteSink();
    DataOutput dataOutput = new DataOutput(fileScan, new CsvTupleWriter(), byteSink);

    dataOutput.open(TestEnvVars.get());
    while (!dataOutput.eos()) {
      TupleBatch tb = dataOutput.nextReady();
    }
    dataOutput.close();

    byte[] responseBytes = ((ByteArrayOutputStream) byteSink.getOutputStream()).toByteArray();
    String dataDst = new String(responseBytes, Charset.forName("UTF-8"));

    assertEquals(dataSrc, dataDst);
  }
}
