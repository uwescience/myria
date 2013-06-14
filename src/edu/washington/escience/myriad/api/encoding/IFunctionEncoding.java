package edu.washington.escience.myriad.api.encoding;

import java.util.List;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.operator.apply.IFunction;

/**
 * 
 * @author leelee
 * 
 * @param <T>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @Type(value = ConstantMultiplicationIFunctionEncoding.class, name = "ConstantMultiplicationIFunction") })
public abstract class IFunctionEncoding<T extends IFunction> extends MyriaApiEncoding {
  public String type;

  /**
   * Instantiate this IFunctionEncoding.
   * 
   * @return this IFunction.
   */
  public abstract T construct();

  /**
   * @return the list of arguments required for this IFunctionEncoding.
   */
  protected abstract List<String> getRequiredArguments();

  @Override
  protected List<String> getRequiredFields() {
    return new ImmutableList.Builder<String>().add("type").addAll(getRequiredArguments()).build();
  }
}