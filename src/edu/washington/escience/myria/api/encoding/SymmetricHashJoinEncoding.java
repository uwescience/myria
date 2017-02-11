package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.SymmetricHashJoin.JoinPullOrder;

public class SymmetricHashJoinEncoding extends BinaryOperatorEncoding<SymmetricHashJoin> {

  @Required public int[] argColumns1;
  @Required public int[] argColumns2;
  @Required public int[] argSelect1;
  @Required public int[] argSelect2;
  public List<String> argColumnNames;
  public boolean argSetSemanticsLeft = false;
  public boolean argSetSemanticsRight = false;
  public JoinPullOrder argOrder = JoinPullOrder.ALTER;

  @Override
  public SymmetricHashJoin construct(final ConstructArgs args) {
    return new SymmetricHashJoin(
        null,
        null,
        argColumns1,
        argColumns2,
        argSelect1,
        argSelect2,
        argSetSemanticsLeft,
        argSetSemanticsRight,
        argColumnNames,
        argOrder);
  }
}
