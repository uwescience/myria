/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.Schema;

/**
 * 
 */
public class FunctionEncoding extends MyriaApiEncoding {
  @Required
  public String name;
  @Required
  public String text;
  @Required
  public FunctionLanguage lang;
  @Required
  public Schema outputSchema;
  @Required
  public Schema inputSchema;

  public Set<Integer> workers;
  public String binary;

  public enum FunctionLanguage {
    /** Postgres UDF */
    POSTGRES,
    /** Python functions */
    PYTHON

  };
}
