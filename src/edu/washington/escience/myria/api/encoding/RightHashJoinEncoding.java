package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.operator.RightHashJoin;
import edu.washington.escience.myria.parallel.Server;

/**
 * 
 * Encoding for {@link RightHashJoin}.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class RightHashJoinEncoding extends BinaryOperatorEncoding<RightHashJoin> {
  public List<String> argColumnNames;
  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;
  @Required
  public int[] argSelect1;
  @Required
  public int[] argSelect2;

  @Override
  public RightHashJoin construct(Server server) {
    return new RightHashJoin(argColumnNames, null, null, argColumns1, argColumns2, argSelect1, argSelect2);
  }
}
