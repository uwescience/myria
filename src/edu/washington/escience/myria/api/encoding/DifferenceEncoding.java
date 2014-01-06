package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.Difference;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class DifferenceEncoding extends OperatorEncoding<Difference> {
  public String argChild1;
  public String argChild2;

  private static final List<String> requiredArguments = ImmutableList.of("argChild1", "argChild2");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public Difference construct(Server server) throws MyriaApiException {
    return new Difference(null, null);
  }

  @Override
  @JsonIgnore
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
