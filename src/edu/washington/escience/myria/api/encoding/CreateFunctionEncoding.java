/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;

/**
 *
 */
public class CreateFunctionEncoding extends MyriaApiEncoding {
  @Required public String name;
  @Required public String outputType;
  @Required public FunctionLanguage lang;
  @Required public Boolean isMultiValued;
  public String description;
  public String binary;
  public Set<Integer> workers;
  public String shortName;
}
