package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.TipsyFileScan;
import edu.washington.escience.myria.parallel.Server;

public class TipsyFileScanEncoding extends LeafOperatorEncoding<TipsyFileScan> {
  @Required
  public String tipsyFilename;
  @Required
  public String grpFilename;
  @Required
  public String iorderFilename;

  @Override
  public TipsyFileScan construct(final Server server) {
    return new TipsyFileScan(tipsyFilename, iorderFilename, grpFilename);
  }

}