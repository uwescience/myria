package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.util.TestEnvVars;

public class SeaFlowFileScanTest {

  @Test
  public void test() throws DbException {
    SeaFlowFileScan scan =
        new SeaFlowFileScan(
            new FileSource(Paths.get("testdata", "seaflow", "1.evt.opp").toString()));
    EmptySink sink = new EmptySink(scan);
    sink.open(TestEnvVars.get());
    while (!sink.eos()) {
      sink.nextReady();
    }
    /* This magic number comes from the first 4 bytes of the file, in little-Endian format. */
    assertEquals(0x5606, sink.getCount());
  }
}
