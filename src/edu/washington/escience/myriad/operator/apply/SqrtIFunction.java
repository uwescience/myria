package edu.washington.escience.myriad.operator.apply;

import java.util.List;

import edu.washington.escience.myriad.Type;

/**
 * SQRT Function sqrt(x).
 */
public class SqrtIFunction extends IFunction {

  /**
   * {@inheritDoc}
   */
  @Override
  public Type getResultType(final List<Type> srcField) {
    return Type.DOUBLE_TYPE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int numApplyFields() {
    return 1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int numExtraArgument() {
    return 0;
  }

  /**
   * Executes the function on src.
   * 
   * @param source a list containing all the values needs to be used
   * @param arguments a list containing all the arguments need to be used
   * @return Math.sqrt(src), return type will always be Double
   */
  @Override
  public Number execute(final List<Number> source, final List<Number> arguments) {
    checkPreconditions(source, numApplyFields());
    return Math.sqrt(source.get(0).doubleValue());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString(final List<String> names, final List<Number> arguments) {
    checkPreconditions(names, numApplyFields());
    return "SQRT(" + names.get(0) + ")";
  }
}
