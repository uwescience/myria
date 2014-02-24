package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Merge;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class MergeEncoding extends OperatorEncoding<Merge> {
  @Required
  public String[] argChildren;

  @Override
  public Merge construct(final Server server) {
    return new Merge(null, null, null);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    Operator[] tmp = new Operator[argChildren.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = operators.get(argChildren[i]);
    }
    current.setChildren(tmp);
  }
}