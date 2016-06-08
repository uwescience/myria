package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Operator;

/**
 * A JSON-able wrapper for the expected wire message for an operator. To add a new operator, three things need to be
 * done.
 *
 * 1. Create an Encoding class that extends OperatorEncoding.
 *
 * 2. Add the operator to the list of (alphabetically sorted) JsonSubTypes below.
 */
public abstract class LeafOperatorEncoding<T extends Operator> extends OperatorEncoding<T> {

  @Override
  public final void connect(Operator current, Map<Integer, Operator> operators) {}
}
