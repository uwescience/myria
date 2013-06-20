package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

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

  @Override
  protected List<String> getRequiredArguments() {
    return ImmutableList.of();
  }
}