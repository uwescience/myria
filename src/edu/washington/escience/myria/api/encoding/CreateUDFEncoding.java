/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

/**
 * 
 */
public class CreateUDFEncoding extends MyriaApiEncoding {
  @Required
  public String udfName;
  @Required
  public String udfDefinition;
  @Required
  public Set<Integer> workers;
}
