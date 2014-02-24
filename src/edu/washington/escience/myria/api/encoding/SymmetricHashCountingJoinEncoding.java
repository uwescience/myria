package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SymmetricHashCountingJoin;
import edu.washington.escience.myria.parallel.Server;

public class SymmetricHashCountingJoinEncoding extends OperatorEncoding<SymmetricHashCountingJoin> {
  @Required
  public String argChild1;
  @Required
  public String argChild2;
  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public SymmetricHashCountingJoin construct(Server server) {
    return new SymmetricHashCountingJoin(null, null, argColumns1, argColumns2);
  }
}