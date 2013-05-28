package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.apply.Apply;
import edu.washington.escience.myriad.operator.apply.IFunctionCaller;

/**
 * 
 * @author leelee
 * 
 */
public class ApplyEncoding extends OperatorEncoding<Apply> {

  public String argChild;
  public List<? extends Number> argConstArgument;
  public IFunctionEncoding<?> argFunction;

  @Override
  public void validate() {
    super.validate();
    try {
      Preconditions.checkNotNull(argChild);
      Preconditions.checkNotNull(argConstArgument);
      Preconditions.checkNotNull(argFunction);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: arg_child, arg_const_argument, arg_function");
    }
  }

  @Override
  public Apply construct() {
    IFunctionCaller caller = new IFunctionCaller(argFunction.construct(), argConstArgument);
    return new Apply(null, ImmutableList.of(caller));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}
