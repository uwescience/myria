package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.SeaFlowFileScan;
import edu.washington.escience.myria.parallel.Server;

public class SeaFlowFileScanEncoding extends LeafOperatorEncoding<SeaFlowFileScan> {
  @Required
  public DataSource source;

  @Override
  public SeaFlowFileScan construct(final Server server) {
    return new SeaFlowFileScan(source);
  }

}