package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.parallel.Server;

public class DupElimEncoding extends OperatorEncoding<StreamingStateWrapper> {

  @Required
  public String argChild;

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    operator.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public StreamingStateWrapper construct(Server server) throws MyriaApiException {
    return new StreamingStateWrapper(null, new DupElim());
  }
}
