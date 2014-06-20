package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SymmetricHashJoin;

public class SymmetricHashJoinEncoding extends BinaryOperatorEncoding<SymmetricHashJoin> {

  public List<String> argColumnNames;
  @Required
  public int[] argColumns1;
  @Required
  public int[] argColumns2;
  @Required
  public int[] argSelect1;
  @Required
  public int[] argSelect2;
  public boolean argSetSemanticsLeft = false;
  public boolean argSetSemanticsRight = false;

  @Override
  public SymmetricHashJoin construct(ConstructArgs args) {
    return new SymmetricHashJoin(argColumnNames, null, null, argColumns1, argColumns2, argSelect1, argSelect2,
        argSetSemanticsLeft, argSetSemanticsRight);
  }

}