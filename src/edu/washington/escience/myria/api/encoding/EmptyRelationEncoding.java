package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.EmptyRelation;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class EmptyRelationEncoding extends OperatorEncoding<EmptyRelation> {
  @Required
  public Schema schema;

  @Override
  public EmptyRelation construct(final Server server) {
    return EmptyRelation.of(schema);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }
}