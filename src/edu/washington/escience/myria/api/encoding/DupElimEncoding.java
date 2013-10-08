package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class DupElimEncoding extends OperatorEncoding<DupElim> {

  public String argChild;
  private static final List<String> requiredArguments = ImmutableList.of("argChild");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public DupElim construct(final Server server) {
    return new DupElim(null);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
