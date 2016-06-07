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
  public int tierSelected;
  @Required
  public ScalingAlgorithmEncoding scalingAlgorithm;
}
