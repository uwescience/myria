package edu.washington.escience.myria.util;

import com.google.common.base.Preconditions;

/**
 * A class that lets you set a constant only before it has been read.
 */
public final class Constants {

  /**
   * Private default constructor.
   */
  private Constants() {
  };

  private static int DEFAULT_BATCH_SIZE = 10 * 1000;

  private static int batchSize = DEFAULT_BATCH_SIZE;
  private static boolean batchSizeRead = false;

  public static void setBatchSize(final int batchSize) {
    if (batchSize == Constants.batchSize) {
      return;
    }
    Preconditions.checkArgument(!batchSizeRead);
    Constants.batchSize = batchSize;
  }

  public static int getBatchSize() {
    batchSizeRead = true;
    return batchSize;
  }
}
