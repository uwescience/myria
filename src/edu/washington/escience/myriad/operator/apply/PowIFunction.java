package edu.washington.escience.myriad.operator.apply;

import java.util.List;

/**
 * Power Function for use in Apply.
 */
public final class PowIFunction extends IFunction {

  /**
   * Accepts only powers of whole numbers {@inheritDoc}.
   */
  @Override
  public Number execute(final List<Number> source, final List<Number> arguments) {
    checkPreconditions(source, numApplyFields());
    checkPreconditions(arguments, numExtraArgument());
    Number src = source.get(0);
    Long power = (Long) arguments.get(0);
    Number retval = Math.pow(src.doubleValue(), power);
    if (src instanceof Long) {
      return retval.longValue();
    } else if (src instanceof Integer) {
      return retval.intValue();
    }
    return retval;
  }

  @Override
  public String toString(final List<String> names, final List<Number> arguments) {
    checkPreconditions(names, numApplyFields());
    checkPreconditions(arguments, numExtraArgument());
    return "POW(" + names.get(0) + "," + arguments.get(0) + ")";
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
    return 1;
  }

}
