package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.LocalCountingJoin;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class LocalCountingJoinEncoding extends OperatorEncoding<LocalCountingJoin> {
  public String argChild1;
  public String argChild2;
  public int[] argColumns1;
  public int[] argColumns2;
  private static final List<String> requiredArguments = ImmutableList.of("argChild1", "argChild2", "argColumns1",
      "argColumns2");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild1), operators.get(argChild2) });
  }

  @Override
  public LocalCountingJoin construct(Server server) {
    return new LocalCountingJoin(null, null, argColumns1, argColumns2);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}