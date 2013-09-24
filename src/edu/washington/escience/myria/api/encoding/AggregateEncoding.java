package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.parallel.Server;

public class AggregateEncoding extends OperatorEncoding<Aggregate> {
  public List<List<String>> argAggOperators;
  public int[] argAggFields;
  public String argChild;
  private static final List<String> requiredFields = ImmutableList.of("argChild", "argAggOperators", "argAggFields");

  @Override
  public Aggregate construct(Server server) {
    int[] ops = deserializeAggregateOperator(argAggOperators);
    return new Aggregate(null, argAggFields, ops);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredFields;
  }

  /**
   * Deserializes Aggregate Operators.
   * 
   * @param ops a list of list of aggregate operations
   * @return an array of operations specified.
   */
  private int[] deserializeAggregateOperator(final List<List<String>> ops) {
    int[] result = new int[ops.size()];
    for (int i = 0; i < ops.size(); i++) {
      List<String> operatorForOneAggField = ops.get(i);
      int operations = 0x0;
      for (String operator : operatorForOneAggField) {
        switch (operator) {
          case "AGG_OP_MIN":
            operations |= Aggregator.AGG_OP_MIN;
            break;
          case "AGG_OP_MAX":
            operations |= Aggregator.AGG_OP_MAX;
            break;
          case "AGG_OP_COUNT":
            operations |= Aggregator.AGG_OP_COUNT;
            break;
          case "AGG_OP_SUM":
            operations |= Aggregator.AGG_OP_SUM;
            break;
          case "AGG_OP_AVG":
            operations |= Aggregator.AGG_OP_AVG;
            break;
          case "AGG_OP_STDEV":
            operations |= Aggregator.AGG_OP_STDEV;
            break;
        }
        result[i] = operations;
      }
    }
    return result;
  }
}