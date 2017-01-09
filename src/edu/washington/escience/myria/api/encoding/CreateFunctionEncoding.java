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
  @Required public String description;
  @Required public String outputSchema;
  @Required public FunctionLanguage lang;
  @Required public Boolean isMultivalued;
  public String binary;
  public Set<Integer> workers;
}
