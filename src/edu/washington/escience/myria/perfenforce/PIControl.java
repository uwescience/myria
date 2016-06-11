/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.perfenforce.encoding.ScalingStatusEncoding;

/**
 * 
 */
public class PIControl implements ScalingAlgorithm {

  double kp;
  double ki;
  int w;
  List<Integer> configs;
  int currentClusterSize;
  int currentPositionIndex;

  double ut;
  List<Double> integralWindowSum;
  List<Double> windowRuntimes;
  List<Double> windowSLAs;

  double recordErrorSum;
  double recordErrorValue;

  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PIControl.class);

  public PIControl(final List<Integer> configs, final int currentClusterSize, final double kp, final double ki,
      final int w) {
    this.kp = kp;
    this.ki = ki;
    this.w = w;
    this.currentClusterSize = currentClusterSize;
    this.configs = configs;

    ut = currentClusterSize;
    windowRuntimes = new ArrayList<Double>();
    windowSLAs = new ArrayList<Double>();
    integralWindowSum = new ArrayList<Double>();
  }

  public void setKP(final double kp) {
    this.kp = kp;
  }

  public void setKI(final double ki) {
    this.ki = ki;
  }

  public void setW(final int w) {
    this.w = w;
  }

  @Override
  public void step(final QueryMetaData currentQuery) {

    if (ut == 4) {
      currentPositionIndex = 0;
    }
    if (ut == 6) {
      currentPositionIndex = 1;
    }
    if (ut == 8) {
      currentPositionIndex = 2;
    }
    if (ut == 10) {
      currentPositionIndex = 3;
    }
    if (ut == 12) {
      currentPositionIndex = 4;
    }

    windowRuntimes.add(currentQuery.runtimes.get(currentPositionIndex));
    windowSLAs.add(currentQuery.slaRuntime);

    if ((currentQuery.id + 1) % w == 0) // at window
    {
      LOGGER.warn("INSIDE WINDOW ID " + currentQuery.id);
      double avgRatios = 0;
      for (int q = 0; q < windowRuntimes.size(); q++) {
        avgRatios += windowRuntimes.get(q) / windowSLAs.get(q);
      }
      avgRatios /= w;

      double currentWindowAverage = avgRatios - 1.0;
      double currentError = kp * currentWindowAverage * ut;

      integralWindowSum.add(ki * currentWindowAverage * ut);
      double errorSum = 0;
      for (double x : integralWindowSum) {
        errorSum += x;
      }
      double new_ut = 4 + errorSum + (currentError);

      recordErrorSum = errorSum;
      LOGGER.warn("INSIDE SUM " + recordErrorSum);
      recordErrorValue = currentError;
      LOGGER.warn("INSIDE Error " + recordErrorValue);

      ut = new_ut;

      windowRuntimes.clear();
      windowSLAs.clear();

      // truncate U and floor
      if (ut > 12) {
        ut = 12;
      }
      if (ut < 4) {
        ut = 4;
      }

      // Round method
      if (ut > 4 && ut < 6) {
        ut = (ut - 4 < 6 - ut) ? 4 : 6;
      } else if (ut > 6 && ut < 8) {
        ut = (ut - 6 < 8 - ut) ? 6 : 8;
      } else if (ut > 8 && ut < 10) {
        ut = (ut - 8 < 10 - ut) ? 8 : 10;
      } else if (ut > 10 && ut < 12) {
        ut = (ut - 10 < 12 - ut) ? 10 : 12;
      }

      currentClusterSize = (int) ut;
      currentPositionIndex = configs.indexOf(currentClusterSize);
      currentWindowAverage = 0;
    }
  }

  @Override
  public int getCurrentClusterSize() {
    return currentClusterSize;
  }

  @Override
  public ScalingStatusEncoding getScalingStatus() {
    ScalingStatusEncoding statusEncoding = new ScalingStatusEncoding();
    statusEncoding.PIControlIntegralErrorSum = recordErrorSum;
    statusEncoding.PIControlProportionalErrorValue = recordErrorValue;
    return statusEncoding;
  }

}
