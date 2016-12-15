/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.nio.ByteBuffer;
import java.util.Set;

import edu.washington.escience.myria.Schema;

/**
 *
 */
public class CreateFunctionEncoding extends MyriaApiEncoding {
  @Required public String name;
  @Required public String definition;
  @Required public Schema outputSchema;
  @Required public edu.washington.escience.myria.MyriaConstants.FunctionLanguage lang;
  public String binary;
  public Set<Integer> workers;
}
