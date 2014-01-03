package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.parallel.Server;

public class SymmetricHashJoinEncoding extends OperatorEncoding<SymmetricHashJoin> {
  public String argChild1;
  public String argChild2;
  public List<String> argColumnNames;
  public int[] argColumns1;
  public int[] argColumns2;
  public int[] argSelect1;
  public int[] argSelect2;
  public boolean argSetSemanticsLeft = false;
  public boolean argSetSemanticsRight = false;
  private static final List<String> requiredArguments = ImmutableList.of("argChild1", "argChild2", "argColumns1",
      "argColumns2", "argSelect1", "argSelect2");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public SymmetricHashJoin construct(Server server) {
    return new SymmetricHashJoin(argColumnNames, null, null, argColumns1, argColumns2, argSelect1, argSelect2,
        argSetSemanticsLeft, argSetSemanticsRight);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}