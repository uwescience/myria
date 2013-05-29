package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.agg.Aggregate;

public class AggregateEncoding extends OperatorEncoding<Aggregate> {
  public int[] argAggOps;
  public int[] argAggFields;
  public String argChild;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild);
      Preconditions.checkNotNull(argAggOps);
      Preconditions.checkNotNull(argAggFields);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_child, arg_agg_ops, arg_agg_fields");
    }
  }

  @Override
  public Aggregate construct() {
    return new Aggregate(null, argAggFields, argAggOps);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}