package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * Experimenter for consistent hashing.
 */
public final class Experimenter {

  /** private constructor. */
  private Experimenter() {
  }

  /**
   * 
   * @param args arguments
   * @throws Throwable Exception
   */
  public static void main(final String[] args) throws Throwable {
    // Generate new data
    RandomDataGenerator.generateRandomData(1000000);
    findSkewnessOfConsistHashingVariesReplicas(100, 48);
    // for (int nodeSize = 12; nodeSize < 100; nodeSize += 12) {
    //
    // }
  }

  private static void findSkewnessOfConsistHashingVariesReplicas(int maxReplica, int numNodes) throws IOException,
      FileNotFoundException {
    String fileName = "consistenthash_result_" + numNodes + ".txt";
    PrintStream chOut = new PrintStream(new File(fileName));
    List<Integer> nodeIds = generateRandomNodes(numNodes);
    for (int i = 1; i < maxReplica; i += 2) {
      ConsistentHash ch = new ConsistentHash(nodeIds, i);
      Scanner scan = new Scanner(new File("test_data.dat"));
      while (scan.hasNextInt()) {
        int data = scan.nextInt();
        ch.add(data);
      }
      System.out.println("CH; replica size = " + i + ", skewness = " + ch.getSkewness());
      chOut.println(ch.getSkewness());
      System.out.println("histogram: " + ch.getDistribution());
      scan.close();
    }
    chOut.close();
  }

  private static void findSkewnessOfConsistHashingVariesNumNodes(int maxNode, int replication) throws IOException,
      FileNotFoundException {
    PrintStream htOut = new PrintStream(new File("hashtable_result.txt"));
    PrintStream chOut = new PrintStream(new File("consistenthash_result.txt"));
    for (int i = 2; i < maxNode; i += 2) {
      List<Integer> nodeIds = generateRandomNodes(i);
      ConsistentHash ch = new ConsistentHash(nodeIds, replication);
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new File("test_data.dat"));
      while (scan.hasNextInt()) {
        int data = scan.nextInt();
        ch.add(data);
        ht.add(data);
      }
      System.out.println("CH; node size = " + i + ", skewness = " + ch.getSkewness());
      System.out.println("HT; node size = " + i + ", skewness = " + ht.getSkewness());
      htOut.println(ht.getSkewness());
      chOut.println(ch.getSkewness());
      scan.close();
    }
    htOut.close();
    chOut.close();
  }

  private static void findSkewnessOfBuckets(int maxNode) throws IOException, FileNotFoundException {
    PrintStream htOut = new PrintStream(new File("hashtable_result.txt"));
    for (int i = 2; i < maxNode; i++) {
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new File("test_data.dat"));
      while (scan.hasNextInt()) {
        int data = scan.nextInt();
        ht.add(data);
      }
      System.out.println("HT; node size = " + i + ", skewness = " + ht.getSkewness());
      htOut.println(ht.getSkewness());
      scan.close();
    }
    htOut.close();
  }

  private static void findSkewnessBasedOnReplicas(int maxReplica) throws IOException, FileNotFoundException {
    int numNodes = 10;
    PrintStream htOut = new PrintStream(new File("hashtable_result.txt"));
    PrintStream chOut = new PrintStream(new File("consistenthash_result.txt"));
    for (int i = 1; i < maxReplica; i++) {
      List<Integer> nodeIds = generateRandomNodes(numNodes);
      ConsistentHash ch = new ConsistentHash(nodeIds, i);
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new File("test_data.dat"));
      while (scan.hasNextInt()) {
        int data = scan.nextInt();
        ch.add(data);
        ht.add(data);
      }
      System.out.println("CH; node size = " + i + ", skewness = " + ch.getSkewness());
      System.out.println("HT; node size = " + i + ", skewness = " + ht.getSkewness());
      htOut.println(ht.getSkewness());
      chOut.println(ch.getSkewness());
      scan.close();
    }
    htOut.close();
    chOut.close();
  }

  private static void simpleExperiment(int numDataPoints) throws IOException, FileNotFoundException {
    RandomDataGenerator.generateRandomData(numDataPoints);
    int bucketSize = 48;
    HashTable ht = new HashTable(bucketSize);
    List<Integer> nodeIds = generateRandomNodes(bucketSize);
    ConsistentHash ch = new ConsistentHash(nodeIds, 10);
    Scanner scan = new Scanner(new File("test_data.dat"));
    int numData = 0;
    while (scan.hasNextInt()) {
      int data = scan.nextInt();
      ht.add(data);
      ch.add(data);
      numData++;
    }
    System.out.println("Total number of data points: " + numData);
    System.out.println("Hash table distribution:\t" + ht.getDistribution());
    System.out.println("Hash table skewness:\t" + ht.getSkewness());
    System.out.println("Consistent Hashing distribution:\t" + ch.getDistribution());
    System.out.println("Consistent Hashing skewness:\t" + ch.getSkewness());

    // clean up the mess
    scan.close();
  }

  private static List<Integer> generateRandomNodes(int numNodes) {
    List<Integer> resultList = new ArrayList<Integer>();
    Random rand = new Random();
    for (int i = 0; i < numNodes; i++) {
      resultList.add(rand.nextInt());
    }
    return resultList;
  }
}
