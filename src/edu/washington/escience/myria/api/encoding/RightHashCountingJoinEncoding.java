package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.RightHashCountingJoin;
import edu.washington.escience.myria.parallel.Server;

/**
 * 
 * Encoding for {@link RightHashCountingJoin}.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class RightHashCountingJoinEncoding extends BinaryOperatorEncoding<RightHashCountingJoin> {
  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;

  @Override
  public RightHashCountingJoin construct(Server server) {
    return new RightHashCountingJoin(null, null, argColumns1, argColumns2);
  }
}
