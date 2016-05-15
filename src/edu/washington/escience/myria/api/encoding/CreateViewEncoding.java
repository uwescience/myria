/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

/**
 * 
 */
public class CreateViewEncoding extends MyriaApiEncoding {
  public String viewName;
  public String viewDefinition;
  public Set<Integer> workers;
}
