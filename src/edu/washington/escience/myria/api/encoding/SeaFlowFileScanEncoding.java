package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.SeaFlowFileScan;

public class SeaFlowFileScanEncoding extends LeafOperatorEncoding<SeaFlowFileScan> {
  @Required
  public DataSource source;

  @Override
  public SeaFlowFileScan construct(@Nonnull ConstructArgs args) {
    return new SeaFlowFileScan(source);
  }

}