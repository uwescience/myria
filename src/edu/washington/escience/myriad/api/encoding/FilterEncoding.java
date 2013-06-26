package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.Filter;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.parallel.Server;

public class FilterEncoding extends OperatorEncoding<Filter> {

  public String argChild;
  public PredicateEncoding<?> argPredicate;
  private static List<String> requiredArguments = ImmutableList.of("argChild", "argPredicate");

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    operator.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public Filter construct(Server server) {
    return new Filter(argPredicate.construct(), null);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {
    argPredicate.validate();
  }
}
