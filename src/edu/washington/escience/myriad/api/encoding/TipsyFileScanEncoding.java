package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.TipsyFileScan;
import edu.washington.escience.myriad.parallel.Server;

public class TipsyFileScanEncoding extends OperatorEncoding<TipsyFileScan> {
  public String tipsyFilename;
  public String grpFilename;
  public String iorderFilename;
  private static final List<String> requiredArguments = ImmutableList.of("tipsyFilename", "grpFilename",
      "iorderFilename");

  @Override
  public TipsyFileScan construct(final Server server) {
    return new TipsyFileScan(tipsyFilename, iorderFilename, grpFilename);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}