package edu.washington.escience.myria.api.encoding;

import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator;

public class AggregateEncoding extends UnaryOperatorEncoding<Aggregate> {
  @Required
  public AggregatorFactory[] aggregators;

  @Override
  public Aggregate construct(ConstructArgs args) {
    return new Aggregate(null, aggregators);
  }

  public static int deserializeAggregateOperators(final List<String> ops) {
    int operations = 0x0;
    for (String operator : ops) {
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
    }
    return operations;
  }

  /**
   * Deserializes Aggregate Operators.
   * 
   * @param ops a list of list of aggregate operations
   * @return an array of operations specified.
   */
  public static final List<List<String>> serializeAggregateOperators(final int[] ops) {
    List<List<String>> result = new ArrayList<List<String>>();
    for (int op : ops) {
      result.add(serializeAggregateOperators(op));
    }
    return result;
  }

  public static final List<String> serializeAggregateOperators(final int op) {
    ArrayList<String> opS = new ArrayList<String>(6);
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
    return opS;
  }
}