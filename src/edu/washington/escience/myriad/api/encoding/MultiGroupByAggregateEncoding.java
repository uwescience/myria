package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.operator.agg.MultiGroupByAggregate;

public class MultiGroupByAggregateEncoding extends OperatorEncoding<MultiGroupByAggregate> {

  public String argChild;
  public int[] argAggFields;
  public List<List<String>> argAggOperators;
  public int[] argGroupFields;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argAggFields);
      Preconditions.checkNotNull(argAggOperators);
      Preconditions.checkNotNull(argGroupFields);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_aggFields, arg_groupFields, arg_opFields");
    }
  }

  @Override
  public MultiGroupByAggregate construct() {
    int[] ops = deserializeAggregateOperator(argAggOperators);
    return new MultiGroupByAggregate(null, argAggFields, argGroupFields, ops);
  }

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    operator.setChildren(new Operator[] { operators.get(argChild) });
  }

  /**
   * Deserializes Aggregate Operators.
   * 
   * @param map the map containing all JSON data
   * @param field the name of the field we should get the data from
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
