package edu.washington.escience.myriad.tipsy;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TipsyFileScan;

public class TipsyFileScanSpeedTest {
  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger(TipsyFileScanSpeedTest.class);

  @Test
  public void tipsyFileScanTest() throws Exception {
    /* The three files */
    File tipsyFile = new File("data_nocommit/tipsy/cosmo50cmb.256g2MbwK.00024");
    File grpFile = new File("data_nocommit/tipsy/cosmo50cmb.256g2MbwK.00024.amiga.grp");
    File orderFile = new File("data_nocommit/tipsy/cosmo50cmb.256g2MbwK.00024.iord");
    /* We can read them, right? */
    assertTrue(tipsyFile.canRead());
    assertTrue(grpFile.canRead());
    assertTrue(orderFile.canRead());

    TipsyFileScan scan =
        new TipsyFileScan(tipsyFile.getAbsolutePath(), orderFile.getAbsolutePath(), grpFile.getAbsolutePath());
    SinkRoot sink = new SinkRoot(scan, 1000 * 1000);
    sink.open(null);
    while (!sink.eos()) {
      sink.nextReady();
    }
    sink.close();
    LOGGER.info("Read {} tuples from the file.", sink.getCount());
  }
}
