package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
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
    int[] ops = AggregateEncoding.deserializeAggregateOperator(argAggOperators);
    return new MultiGroupByAggregate(null, argAggFields, argGroupFields, ops);
  }

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    operator.setChildren(new Operator[] { operators.get(argChild) });
  }

}
