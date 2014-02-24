package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import edu.washington.escience.myria.operator.MergeJoin;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class MergeJoinEncoding extends OperatorEncoding<MergeJoin> {
  @Required
  public String argChild1;
  @Required
  public String argChild2;
  public List<String> argColumnNames;
  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;
  @Required
  public int[] argSelect1;
  @Required
  public int[] argSelect2;
  @Required
  public boolean[] acending;

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public MergeJoin construct(Server server) {
    return new MergeJoin(argColumnNames, null, null, argColumns1, argColumns2, argSelect1, argSelect2, acending);
  }

}