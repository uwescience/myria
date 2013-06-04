package edu.washington.escience.myriad.api.encoding;

import edu.washington.escience.myriad.operator.apply.ConstantMultiplicationIFunction;

/**
 * 
 * @author leelee
 * 
 */
public class ConstantMultiplicationIFunctionEncoding extends IFunctionEncoding<ConstantMultiplicationIFunction> {

  @Override
  public ConstantMultiplicationIFunction construct() {
    return new ConstantMultiplicationIFunction();
  }
}