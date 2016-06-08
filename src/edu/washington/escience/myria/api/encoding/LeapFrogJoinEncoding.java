package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.LeapFrogJoin;

public class LeapFrogJoinEncoding extends NaryOperatorEncoding<LeapFrogJoin> {

  public List<String> argColumnNames;
  public boolean[] indexOnFirst;
  @Required public int[][][] joinFieldMapping;
  @Required public int[][] outputFieldMapping;

  @Override
  public LeapFrogJoin construct(ConstructArgs args) throws MyriaApiException {
    return new LeapFrogJoin(
        null, joinFieldMapping, outputFieldMapping, argColumnNames, indexOnFirst);
  }
}
