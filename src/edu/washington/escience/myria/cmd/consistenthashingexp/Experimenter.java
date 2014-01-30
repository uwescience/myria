package edu.washington.escience.myria.cmd.consistenthashingexp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
    // RandomDataGenerator.generateRandomData(1000000);
    // experimentToProveONotation(100, 10000);
    // experimentToProveONotation(10000, 100);
    Integer[] workersArray = { 0, 1, 2 };
    Integer[] datapoints = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    getResultsAfterConsistentHashing(Arrays.asList(workersArray), Arrays.asList(datapoints), 3);
  }

  /**
   * Prints out the data distribution after consistent hashing. This method is good for a small numbers of datapoints.
   * 
   */
  private static void getResultsAfterConsistentHashing(final List<Integer> nodes, final List<Integer> datapoints,
      final int numReplica) {
    ConsistentHash ch = new ConsistentHash(nodes, numReplica);
    for (Integer datapoint : datapoints) {
      ch.add(datapoint);
    }
    System.out.println(ch.getDistribution());
    System.out.println(ch.getCircle());
  }

  /**
   * We want to prove that if numNodes * numReplica is very large, the skew should be minimal so we set numNodes *
   * numReplica = 1m.
   * 
   * Then, see the effect of switching between nodeSize and repSize
   * 
   * @throws IOException exception
   */
  private static void experimentToProveONotation(final int nodeSize, final int repSize) throws IOException {
    double skew = findSkewnessOfConsistentHashing(nodeSize, repSize);
    System.out.println("With node = " + nodeSize + " and numReplica = " + repSize + ", skew = " + skew);
  }

  /**
   * Finds the skewness of data in consistent hashing with the specified number of nodes, replica size.
   * 
   * @param numNodes the number of nodes
   * @param numReplica the number of replicas
   * @return the data skew
   * @throws IOException exception
   */
  private static double findSkewnessOfConsistentHashing(final int numNodes, final int numReplica) throws IOException {
    ConsistentHash ch = new ConsistentHash(generateRandomNodes(numNodes), numReplica);
    Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
    int count;
    for (count = 0; scan.hasNextInt(); ++count) {
      ch.add(scan.nextInt());
    }
    scan.close();
    return ch.getSkewness();
  }

  private static void findSkewnessOfConsistHashingVariesReplicas(int maxReplica, int numNodes) throws IOException,
      FileNotFoundException {
    String fileName = "consistenthash_result_" + numNodes + ".txt";
    PrintWriter chOut = new PrintWriter(new BufferedWriter(new FileWriter(fileName)));
    for (int replicaSize = 1; replicaSize <= maxReplica; replicaSize += 2) {
      chOut.println(findSkewnessOfConsistentHashing(numNodes, replicaSize));
    }
    chOut.close();
  }

  private static void findSkewnessOfConsistHashingVariesNumNodes(int maxNode, int replication) throws IOException,
      FileNotFoundException {
    PrintWriter htOut = new PrintWriter(new BufferedWriter(new FileWriter("hashtable_result.txt")));
    PrintWriter chOut = new PrintWriter(new BufferedWriter(new FileWriter("consistenthash_result.txt")));
    for (int i = 2; i < maxNode; i += 2) {
      List<Integer> nodeIds = generateRandomNodes(i);
      ConsistentHash ch = new ConsistentHash(nodeIds, replication);
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
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
    PrintWriter htOut = new PrintWriter(new BufferedWriter(new FileWriter("hashtable_result.txt")));
    for (int i = 2; i < maxNode; i++) {
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
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
    PrintWriter htOut = new PrintWriter(new BufferedWriter(new FileWriter("hashtable_result.txt")));
    PrintWriter chOut = new PrintWriter(new BufferedWriter(new FileWriter("consistenthash_result.txt")));
    for (int i = 1; i < maxReplica; i++) {
      List<Integer> nodeIds = generateRandomNodes(numNodes);
      ConsistentHash ch = new ConsistentHash(nodeIds, i);
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
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

  private static void findSkewnessByVaryingNumNodes(int numNodes) throws IOException, FileNotFoundException {
    PrintWriter htOut = new PrintWriter(new BufferedWriter(new FileWriter("hashtable_result.txt")));
    PrintWriter chOut = new PrintWriter(new BufferedWriter(new FileWriter("consistenthash_result.txt")));
    for (int i = 1; i < numNodes; i++) {
      ConsistentHashingWithGuava ch = new ConsistentHashingWithGuava(i);
      HashTable ht = new HashTable(i);
      Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
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

  private static void dataMovementExperiment(final int initial, final int after) throws IOException,
      FileNotFoundException {
    ConsistentHashingWithGuava ch = new ConsistentHashingWithGuava(initial);
    Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
    // populate the data structure
    while (scan.hasNextInt()) {
      int data = scan.nextInt();
      ch.add(data);
    }
    ch.addNode(after);
    System.out.println("The moving statistics: " + ch.getDataMoveStats());
    scan.close();
  }

  private static void simpleExperimentWithOwnCH(int numDataPoints) throws IOException, FileNotFoundException {
    RandomDataGenerator.generateRandomUniformData(numDataPoints);
    int bucketSize = 48;
    HashTable ht = new HashTable(bucketSize);
    List<Integer> nodeIds = generateRandomNodes(bucketSize);
    ConsistentHash ch = new ConsistentHash(nodeIds, 10);
    Scanner scan = new Scanner(new BufferedReader(new FileReader(RandomDataGenerator.FILENAME)));
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
