/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 */
public interface ScalingAlgorithm {

  public void step(QueryMetaData q);

  public int getCurrentClusterSize();

  public ScalingStatusEncoding getScalingStatus();

}
