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
  @Required public String name;
  @Required public String definition;
  @Required public Schema outputSchema;
  public Set<Integer> workers;
}
