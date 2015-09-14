package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.RightHashCountingJoin;

/**
 * 
 * Encoding for {@link RightHashCountingJoin}.
 * 
 */
public class RightHashCountingJoinEncoding extends BinaryOperatorEncoding<RightHashCountingJoin> {
  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;

  @Override
  public RightHashCountingJoin construct(@Nonnull ConstructArgs args) {
    return new RightHashCountingJoin(null, null, argColumns1, argColumns2);
  }
}
