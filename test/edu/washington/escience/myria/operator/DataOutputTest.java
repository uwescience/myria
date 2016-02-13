/**
 *
 */
package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.ByteSink;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * 
 */
public class DataOutputTest {

  @Test
  public void DataOutputTest() throws Exception {
    /* Read a CSV and construct the query */
    final String filename = "testdata/twitter/TwitterK.csv";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));

    /* Build the query and verify */
    FileScan scanCSV = new FileScan(new FileSource(filename), schema);
    DataSink byteSink = new ByteSink();
    DataOutput dataOutput = new DataOutput(scanCSV, new CsvTupleWriter(), byteSink);

    dataOutput.open(TestEnvVars.get());
    int actualTupleCount = 0;
    while (!dataOutput.eos()) {
      TupleBatch tb = dataOutput.nextReady();
      if (tb != null) {
        actualTupleCount += tb.numTuples();
      }
    }
    dataOutput.close();

    int expectedTupleCount = 2715;
    assertEquals(actualTupleCount, expectedTupleCount);

  }
}
