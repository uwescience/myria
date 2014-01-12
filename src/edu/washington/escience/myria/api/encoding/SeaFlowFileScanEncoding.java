package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SeaFlowFileScan;
import edu.washington.escience.myria.parallel.Server;

public class SeaFlowFileScanEncoding extends OperatorEncoding<SeaFlowFileScan> {
  public DataSource source;
  private static final List<String> requiredArguments = ImmutableList.of("source");

  @Override
  public SeaFlowFileScan construct(final Server server) {
    return new SeaFlowFileScan(source);
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