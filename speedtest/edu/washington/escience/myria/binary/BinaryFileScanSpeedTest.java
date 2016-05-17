package edu.washington.escience.myria.binary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.operator.BinaryFileScan;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.tipsy.TipsyFileScanSpeedTest;

public class BinaryFileScanSpeedTest extends AbstractBenchmark {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(TipsyFileScanSpeedTest.class);

  @BenchmarkOptions(benchmarkRounds = 5, warmupRounds = 1)
  @Test
  public void binaryFileScanTest() throws Exception {
    File binaryFile =
        new File("data_nocommit/speedtest/dbcosmo50/cosmo50cmb.256g2bwK.00512.star.bin");
    /* We can read the file, right? */
    assertTrue(binaryFile.canRead());

    Type[] typeAr = {
      Type.LONG_TYPE, // iOrder
      Type.FLOAT_TYPE, // mass
      Type.FLOAT_TYPE, // x
      Type.FLOAT_TYPE, // y
      Type.FLOAT_TYPE, // z
      Type.FLOAT_TYPE, // vx
      Type.FLOAT_TYPE, // vy
      Type.FLOAT_TYPE, // vz
      Type.FLOAT_TYPE, // metals
      Type.FLOAT_TYPE, // tform
      Type.FLOAT_TYPE, // eps
      Type.FLOAT_TYPE, // phi
    };
    Schema schema = new Schema(Arrays.asList(typeAr));

    BinaryFileScan scan = new BinaryFileScan(schema, new FileSource(binaryFile.getAbsolutePath()));
    SinkRoot sink = new SinkRoot(scan);
    sink.open(null);
    while (!sink.eos()) {
      sink.nextReady();
    }
    sink.close();
    LOGGER.info("Read {} tuples from the file.", sink.getCount());
    assertEquals(12417544, sink.getCount());
  }
}
