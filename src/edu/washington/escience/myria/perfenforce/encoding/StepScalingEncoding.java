package edu.washington.escience.myria.perfenforce.encoding;

import edu.washington.escience.myria.api.encoding.Required;

/**
 * 
 */
public class StepScalingEncoding {
  @Required
  public String name;
  @Required
  public String pathName;
  @Required
  public ScalingAlgorithmEncoding scalingAlgorithm;

  // Optional for replay
  public String querySequence;
}
