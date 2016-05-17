package edu.washington.escience.myria.file;

import static org.junit.Assert.assertEquals;

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

public class FileScanSpeedTest extends AbstractBenchmark {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(FileScanSpeedTest.class);

  @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 1)
  @Test
  public void fileScanTest() throws Exception {
    Type[] typeAr = {Type.INT_TYPE, Type.INT_TYPE, Type.FLOAT_TYPE, Type.STRING_TYPE};
    Schema schema = new Schema(Arrays.asList(typeAr));

    // generated using:
    // python testdata/generated/generate_csv.py 10000000 int int float str > data_nocommit/speedtest/random.csv
    FileScan scan = new FileScan("data_nocommit/speedtest/random.csv", schema);

    SinkRoot sink = new SinkRoot(scan);
    sink.open(null);
    while (!sink.eos()) {
      sink.nextReady();
    }
    sink.close();
    LOGGER.info("Read {} tuples from the file.", sink.getCount());
    assertEquals(10000000, sink.getCount());
  }
}
