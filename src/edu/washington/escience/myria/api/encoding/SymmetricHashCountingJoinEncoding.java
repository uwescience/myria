package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SymmetricHashCountingJoin;

public class SymmetricHashCountingJoinEncoding
    extends BinaryOperatorEncoding<SymmetricHashCountingJoin> {

  @Required public int[] argColumns1;
  @Required public int[] argColumns2;

  @Override
  public SymmetricHashCountingJoin construct(ConstructArgs args) {
    return new SymmetricHashCountingJoin(null, null, argColumns1, argColumns2);
  }
}
