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

  /**
   * The default batch size that is used if no other batch size is set.
   */
  private static final int DEFAULT_BATCH_SIZE = 10 * 1000;

  /**
   * The lazily initialized batch size variable.
   */
  private static int batchSize = DEFAULT_BATCH_SIZE;

  /**
   * Set to true if {@link #getBatchSize()} has been called.
   */
  private static boolean batchSizeRead = false;

  /**
   * Set the batch size before it has been read the first time. It is okay to set the batch size to the current value
   * even though it has been read.
   * 
   * @param batchSize the batch size
   */
  public static void setBatchSize(final int batchSize) {
    if (batchSize == Constants.batchSize) {
      return;
    }
    Preconditions.checkArgument(!batchSizeRead, "You cannot edit the batch size because it has been read.");
    Constants.batchSize = batchSize;
  }

  /**
   * Get the batch size. This also sets {@link #batchSizeRead} to avoid further modifications.
   * 
   * @return the batch size
   */
  public static int getBatchSize() {
    batchSizeRead = true;
    return batchSize;
  }
}
