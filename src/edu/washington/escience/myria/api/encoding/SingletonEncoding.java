package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SingletonRelation;
import edu.washington.escience.myria.parallel.Server;

public class SingletonEncoding extends OperatorEncoding<SingletonRelation> {
  @Override
  public SingletonRelation construct(final Server server) {
    return new SingletonRelation();
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  protected List<String> getRequiredArguments() {
    return ImmutableList.of();
  }
}