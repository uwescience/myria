/**
 *
 */
package edu.washington.escience.myria.perfenforce.encoding;

import edu.washington.escience.myria.api.encoding.Required;

/**
 * 
 */
public class InitializeScalingEncoding {
  @Required
  public int tier;
  @Required
  public String path;
  @Required
  public ScalingAlgorithmEncoding scalingAlgorithm;
}
