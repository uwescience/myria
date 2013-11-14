package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

/**
 * Utility class for generating random integer data The utility can generate any arbitrary number of data points.
 */
public final class RandomDataGenerator {

  /** The output filename. */
  public static final String FILENAME = "test_data.dat";

  /** hide the constructor. */
  private RandomDataGenerator() {
  }

  /**
   * @param args arguments
   * @throws Throwable Exception
   */
  public static void main(final String[] args) throws Throwable {
    Scanner scan = new Scanner(System.in);
    System.out.print("Number of data points to generate: ");
    int numDataPoint = scan.nextInt();
    generateRandomSkewedGaussianData(numDataPoint, 10000, 100000);
    scan.close();
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @throws IOException IOException
   */
  public static void generateRandomUniformData(final int numDataPoint) throws IOException {
    Random randomizer = new Random(System.currentTimeMillis());
    generateRandomUniformDataHelper(numDataPoint, randomizer);
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param randomSeed the seed for the random generator
   * @throws IOException IOException
   */
  public static void generateRandomUniformData(final int numDataPoint, final int randomSeed) throws IOException {
    Random randomizer = new Random(randomSeed);
    generateRandomUniformDataHelper(numDataPoint, randomizer);
  }

  /**
   * Generates random skewed data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param mean the value we are skewwing towards to
   * @param stdev the standard deviation
   * @throws IOException IOException
   */
  public static void generateRandomSkewedGaussianData(final int numDataPoint, final int mean, final int stdev)
      throws IOException {
    Random randomizer = new Random(System.currentTimeMillis());
    generateRandomSkewedGaussianDataHelper(numDataPoint, randomizer, mean, stdev);
  }

  /**
   * Generates random skewed data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param randomSeed the seed for the random generator
   * @param mean the value we are skewwing towards to
   * @param stdev the standard deviation
   * @throws IOException IOException
   */
  public static void generateRandomSkewedGaussianData(final int numDataPoint, final int randomSeed, final int mean,
      final int stdev) throws IOException {
    Random randomizer = new Random(randomSeed);
    generateRandomSkewedGaussianDataHelper(numDataPoint, randomizer, mean, stdev);
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param randomizer the randomizer
   * @throws IOException IOException
   */
  private static void generateRandomUniformDataHelper(final int numDataPoint, final Random randomizer)
      throws IOException {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(FILENAME)));
    System.out.println("Generating data...");
    for (int i = 0; i < numDataPoint; i++) {
      // generate in a smaller range?
      out.println(randomizer.nextInt());
    }
    System.out.println("Done generating " + numDataPoint + " random data points");
    out.close();
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param randomizer the randomizer
   * @param mean the value to skew towards
   * @param stdev the standard deviation
   * @throws IOException IOException
   */
  private static void generateRandomSkewedGaussianDataHelper(final int numDataPoint, final Random randomizer,
      final int mean, final int stdev) throws IOException {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(FILENAME)));
    Set<Integer> distValues = new HashSet<Integer>();
    System.out.println("Generating data...");
    for (int i = 0; i < numDataPoint; i++) {
      // generate in a smaller range?
      int value = (int) (mean + randomizer.nextGaussian() * stdev);
      out.println(value);
      distValues.add(value);
    }
    System.out.println("Done generating " + numDataPoint + " random data points");
    System.out.println("Number Unique Datapoints " + distValues.size());
    out.close();
  }
}
