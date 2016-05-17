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
public abstract class NaryOperatorEncoding<T extends Operator> extends OperatorEncoding<T> {

  @Required public Integer[] argChildren;

  @Override
  public final void connect(final Operator current, final Map<Integer, Operator> operators) {
    Operator[] tmp = new Operator[argChildren.length];
    for (int i = 0; i < tmp.length; ++i) {
      tmp[i] = operators.get(argChildren[i]);
    }
    current.setChildren(tmp);
  }
}
