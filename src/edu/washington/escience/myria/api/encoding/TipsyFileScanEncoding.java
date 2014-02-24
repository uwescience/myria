package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.TipsyFileScan;
import edu.washington.escience.myria.parallel.Server;

public class TipsyFileScanEncoding extends OperatorEncoding<TipsyFileScan> {
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

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

}