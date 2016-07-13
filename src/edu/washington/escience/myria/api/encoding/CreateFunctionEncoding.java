/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.Schema;

/**
 *
 */
public class CreateFunctionEncoding extends MyriaApiEncoding {
  @Required public String functionName;
  @Required public String functionDefinition;
  @Required public Schema functionOutputSchema;
  public Set<Integer> workers;
}
