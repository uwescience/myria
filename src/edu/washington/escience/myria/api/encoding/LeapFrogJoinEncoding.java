package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.LeapFrogJoin;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class LeapFrogJoinEncoding extends OperatorEncoding<LeapFrogJoin> {

  public List<String> argChildren;
  public List<String> argColumnNames;
  public int[][][] joinFieldMapping;
  public int[][] outputFieldMapping;

  private static final List<String> requiredArguments = ImmutableList.of("argChildren", "joinFieldMapping",
      "outputFieldMapping");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    Operator[] childOperators = new Operator[argChildren.size()];
    for (int i = 0; i < childOperators.length; ++i) {
      childOperators[i] = operators.get(argChildren.get(i));
    }
    current.setChildren(childOperators);
  }

  @Override
  public LeapFrogJoin construct(Server server) throws MyriaApiException {
    return new LeapFrogJoin(null, joinFieldMapping, outputFieldMapping, argColumnNames);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

}
