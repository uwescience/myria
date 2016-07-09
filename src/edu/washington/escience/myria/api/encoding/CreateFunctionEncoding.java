/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

/**
 *
 */
public class CreateFunctionEncoding extends MyriaApiEncoding {
  @Required public String functionName;
  @Required public String functionDefinition;
  public Set<Integer> workers;
}
