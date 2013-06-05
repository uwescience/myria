package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Filter;
import edu.washington.escience.myriad.operator.Operator;

public class FilterEncoding extends OperatorEncoding<Filter> {

  public String argChild;
  public PredicateEncoding<?> argPredicate;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild);
      Preconditions.checkNotNull(argPredicate);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_child, arg_predicate");
    }
  }

  @Override
  public Filter construct() {
    return new Filter(argPredicate.construct(), null);
  }

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    operator.setChildren(new Operator[] { operators.get(argChild) });
  }

}
