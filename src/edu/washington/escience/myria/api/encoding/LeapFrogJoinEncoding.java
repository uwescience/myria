package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.LeapFrogJoin;
import edu.washington.escience.myria.parallel.Server;

public class LeapFrogJoinEncoding extends NaryOperatorEncoding<LeapFrogJoin> {

  public List<String> argColumnNames;
  public Boolean[] indexOnFirst;
  @Required
  public int[][][] joinFieldMapping;
  @Required
  public int[][] outputFieldMapping;

  @Override
  public LeapFrogJoin construct(Server server) throws MyriaApiException {
    return new LeapFrogJoin(null, joinFieldMapping, outputFieldMapping, argColumnNames, indexOnFirst);
  }
}
