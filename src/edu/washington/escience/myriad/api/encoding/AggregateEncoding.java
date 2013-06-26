package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.agg.Aggregate;
import edu.washington.escience.myriad.parallel.Server;

public class AggregateEncoding extends OperatorEncoding<Aggregate> {
  public int[] argAggOps;
  public int[] argAggFields;
  public String argChild;
  private static final List<String> requiredFields = ImmutableList.of("argChild", "argAggOps", "argAggFields");

  @Override
  public Aggregate construct(Server server) {
    return new Aggregate(null, argAggFields, argAggOps);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredFields;
  }
}