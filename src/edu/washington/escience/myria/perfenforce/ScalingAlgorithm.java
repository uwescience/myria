/**
 *
 */
package edu.washington.escience.myria.perfenforce;

/**
 */
public interface ScalingAlgorithm {

  public void step();

  public int getCurrentClusterSize();

}
