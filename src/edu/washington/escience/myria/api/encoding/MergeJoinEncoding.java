package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.MergeJoin;

public class MergeJoinEncoding extends BinaryOperatorEncoding<MergeJoin> {
  public List<String> argColumnNames;
  @Required public int[] argColumns1;
  @Required public int[] argColumns2;
  @Required public int[] argSelect1;
  @Required public int[] argSelect2;
  @Required public boolean[] acending;

  @Override
  public MergeJoin construct(ConstructArgs args) {
    return new MergeJoin(
        argColumnNames, null, null, argColumns1, argColumns2, argSelect1, argSelect2, acending);
  }
}
