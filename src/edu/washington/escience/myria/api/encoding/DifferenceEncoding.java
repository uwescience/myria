package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.Difference;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class DifferenceEncoding extends OperatorEncoding<Difference> {
  @Required
  public String argChild1;
  @Required
  public String argChild2;

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public Difference construct(Server server) throws MyriaApiException {
    return new Difference(null, null);
  }
}
