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
  public double alpha;
  public double beta;
  public double kp;
  public double ki;
  public int w;
  public double lr;
}
