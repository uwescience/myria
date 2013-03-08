package edu.washington.escience.myriad.operator.apply;

import java.util.List;

/**
 * Power Function for use in Apply
 * 
 * @param <Tin>
 *          Type of the data being fed and output
 */
public class PowIFunction extends IFunction {

  /**
   * creates the Power function with the desired power value
   * 
   */
  public PowIFunction() {

  }

  /**
   * Accepts only powers of whole numbers {@inheritDoc}
   */
  @Override
  public Number execute(List<Number> source, List<Number> arguments) {
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
  public String toString(List<String> names, List<Number> arguments) {
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
