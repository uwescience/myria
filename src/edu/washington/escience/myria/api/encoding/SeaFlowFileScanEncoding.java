package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SeaFlowFileScan;
import edu.washington.escience.myria.parallel.Server;

public class SeaFlowFileScanEncoding extends OperatorEncoding<SeaFlowFileScan> {
  @Required
  public DataSource source;

  @Override
  public SeaFlowFileScan construct(final Server server) {
    return new SeaFlowFileScan(source);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }
}