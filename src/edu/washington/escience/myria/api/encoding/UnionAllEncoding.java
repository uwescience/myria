package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.parallel.Server;

public class UnionAllEncoding extends OperatorEncoding<UnionAll> {
  @Required
  public String[] argChildren;

  @Override
  public UnionAll construct(final Server server) {
    return new UnionAll(null);
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