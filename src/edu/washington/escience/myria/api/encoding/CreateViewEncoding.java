/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

/**
 *
 */
public class CreateViewEncoding extends MyriaApiEncoding {
  @Required public String viewName;
  @Required public String viewDefinition;
  public Set<Integer> workers;
}
