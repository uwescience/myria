package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.apply.Apply;
import edu.washington.escience.myria.operator.apply.IFunctionCaller;
import edu.washington.escience.myria.parallel.Server;

/**
 * 
 * @author leelee
 * 
 */
public class ApplyEncoding extends OperatorEncoding<Apply> {

  public String argChild;
  public List<? extends Number> argConstArgument;
  public IFunctionEncoding<?> argFunction;
  private static final ImmutableList<String> requiredArguments = ImmutableList.of("argChild", "argConstArgument",
      "argFunction");

  @Override
  public Apply construct(Server server) {
    IFunctionCaller caller = new IFunctionCaller(argFunction.construct(), argConstArgument);
    return new Apply(null, ImmutableList.of(caller));
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {
    argFunction.validate();
  }
}
