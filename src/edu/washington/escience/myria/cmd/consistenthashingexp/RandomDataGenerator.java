package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.Scanner;

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
    generateRandomData(numDataPoint);
    scan.close();
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @throws IOException IOException
   */
  public static void generateRandomData(final int numDataPoint) throws IOException {
    Random randomizer = new Random(System.currentTimeMillis());
    generateRandomDataHelper(numDataPoint, randomizer);
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param randomSeed the seed for the random generator
   * @throws IOException IOException
   */
  public static void generateRandomData(final int numDataPoint, final int randomSeed) throws IOException {
    Random randomizer = new Random(randomSeed);
    generateRandomDataHelper(numDataPoint, randomizer);
  }

  /**
   * Generates random data points in a file.
   * 
   * @param numDataPoint number of data points
   * @param randomizer the randomizer
   * @throws IOException IOException
   */
  private static void generateRandomDataHelper(final int numDataPoint, final Random randomizer) throws IOException {
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(FILENAME)));
    System.out.print("Generating data...");
    for (int i = 0; i < numDataPoint; i++) {
      // generate in a smaller range?
      out.println(randomizer.nextInt());
    }
    System.out.println("Done generating " + numDataPoint + " random data points");
    out.close();
  }

}
