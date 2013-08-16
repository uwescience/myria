package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.Server;

public class SinkRootEncoding extends OperatorEncoding<SinkRoot> {

  public String argChild;
  public Integer argLimit;
  private static final List<String> requiredArguments = ImmutableList.of("argChild");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public SinkRoot construct(Server server) {
    if (argLimit != null) {
      return new SinkRoot(null, argLimit);
    } else {
      return new SinkRoot(null);
    }
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}