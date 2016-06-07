/**
 *
 */
package edu.washington.escience.myria.perfenforce.encoding;

import edu.washington.escience.myria.api.encoding.Required;

/**
 * 
 */
public class ScalingAlgorithmEncoding {
  @Required
  public String name;

  // Optional Params
  public int alpha;
  public int beta;
  public int kp;
  public int ki;
  public int lr;
}
