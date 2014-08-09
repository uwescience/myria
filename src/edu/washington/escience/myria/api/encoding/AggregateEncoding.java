package edu.washington.escience.myria.api.encoding;

import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator;

public class AggregateEncoding extends UnaryOperatorEncoding<Aggregate> {
  @Required
  public List<List<String>> argAggOperators;
  @Required
  public int[] argAggFields;

  @Override
  public Aggregate construct(ConstructArgs args) {
    int[] ops = deserializeAggregateOperator(argAggOperators);
    return new Aggregate(null, argAggFields, ops);
  }

  /**
   * Deserializes Aggregate Operators.
   * 
   * @param ops a list of list of aggregate operations
   * @return an array of operations specified.
   */
  public static int[] deserializeAggregateOperator(final List<List<String>> ops) {
    int[] result = new int[ops.size()];
    for (int i = 0; i < ops.size(); i++) {
      List<String> operatorForOneAggField = ops.get(i);
      int operations = 0x0;
      for (String operator : operatorForOneAggField) {
        switch (operator.toUpperCase()) {
          case "AGG_OP_MIN":
          case "MIN":
            operations |= PrimitiveAggregator.AGG_OP_MIN;
            break;
          case "AGG_OP_MAX":
          case "MAX":
            operations |= PrimitiveAggregator.AGG_OP_MAX;
            break;
          case "AGG_OP_COUNT":
          case "COUNT":
            operations |= PrimitiveAggregator.AGG_OP_COUNT;
            break;
          case "AGG_OP_SUM":
          case "SUM":
            operations |= PrimitiveAggregator.AGG_OP_SUM;
            break;
          case "AGG_OP_AVG":
          case "AVG":
            operations |= PrimitiveAggregator.AGG_OP_AVG;
            break;
          case "AGG_OP_STDEV":
          case "STDEV":
            operations |= PrimitiveAggregator.AGG_OP_STDEV;
            break;
        }
        result[i] = operations;
      }
    }
    return result;
  }

  /**
   * Deserializes Aggregate Operators.
   * 
   * @param ops a list of list of aggregate operations
   * @return an array of operations specified.
   */
  public static final List<List<String>> serializeAggregateOperator(final int[] ops) {
    List<List<String>> result = new ArrayList<List<String>>();
    for (int op : ops) {
      ArrayList<String> opS = new ArrayList<String>(6);
      result.add(opS);
      if ((op & PrimitiveAggregator.AGG_OP_MIN) != 0) {
        opS.add("MIN");
      }
      if ((op & PrimitiveAggregator.AGG_OP_MAX) != 0) {
        opS.add("MAX");
      }
      if ((op & PrimitiveAggregator.AGG_OP_COUNT) != 0) {
        opS.add("COUNT");
      }
      if ((op & PrimitiveAggregator.AGG_OP_SUM) != 0) {
        opS.add("SUM");
      }
      if ((op & PrimitiveAggregator.AGG_OP_AVG) != 0) {
        opS.add("AVG");
      }
      if ((op & PrimitiveAggregator.AGG_OP_STDEV) != 0) {
        opS.add("STDEV");
      }
    }
    return result;
  }
}