package edu.washington.escience.myria.file;

import java.util.Arrays;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.tipsy.TipsyFileScanSpeedTest;

public class FileScanSpeedTest extends AbstractBenchmark {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(TipsyFileScanSpeedTest.class);

  @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
  @Test
  public void binaryFileScanTest() throws Exception {
    Type[] typeAr = { Type.INT_TYPE, // followee
        Type.INT_TYPE, // follower
    };
    Schema schema = new Schema(Arrays.asList(typeAr));

    FileScan scan = new FileScan("hdfs://vega.cs.washington.edu:8020//datasets/twitter/twitter_rv.net", schema, "\t");
    SinkRoot sink = new SinkRoot(scan);
    sink.open(null);
    while (!sink.eos()) {
      sink.nextReady();
    }
    sink.close();
    LOGGER.info("Read {} tuples from the file.", sink.getCount());
    // assertEquals(12417544, sink.getCount());
  }
}
