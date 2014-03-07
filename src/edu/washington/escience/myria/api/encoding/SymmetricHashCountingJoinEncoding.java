package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.SymmetricHashCountingJoin;
import edu.washington.escience.myria.parallel.Server;

public class SymmetricHashCountingJoinEncoding extends BinaryOperatorEncoding<SymmetricHashCountingJoin> {

  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;

  @Override
  public SymmetricHashCountingJoin construct(Server server) {
    return new SymmetricHashCountingJoin(null, null, argColumns1, argColumns2);
  }
}