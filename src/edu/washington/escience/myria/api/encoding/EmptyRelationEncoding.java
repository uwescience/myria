package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.EmptyRelation;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class EmptyRelationEncoding extends OperatorEncoding<EmptyRelation> {
  public Schema schema;
  private static final List<String> requiredArguments = ImmutableList.of("schema");

  @Override
  public EmptyRelation construct(final Server server) {
    return EmptyRelation.of(schema);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}