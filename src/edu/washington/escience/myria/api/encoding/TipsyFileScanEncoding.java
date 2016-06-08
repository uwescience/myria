package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.TipsyFileScan;

public class TipsyFileScanEncoding extends LeafOperatorEncoding<TipsyFileScan> {
  @Required public String tipsyFilename;
  @Required public String grpFilename;
  @Required public String iorderFilename;

  @Override
  public TipsyFileScan construct(ConstructArgs args) {
    return new TipsyFileScan(tipsyFilename, iorderFilename, grpFilename);
  }
}
