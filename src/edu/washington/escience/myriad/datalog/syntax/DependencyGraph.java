package edu.washington.escience.myriad.datalog.syntax;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * This class prints the Dependency Graph for the rules of a Datalog program.
 * 
 * @author mar, dhalperi
 * 
 */
public final class DependencyGraph {
  /** The Datalog program that this graph represents. */
  private final DatalogProgram program;

  /** A list of the EDBs in the given Datalog program. */
  private final List<String> edbs;
  /** Maps each predicate into a list of the predicates that it depends on. */
  private final HashMap<String, LinkedHashSet<String>> depGraph = new HashMap<String, LinkedHashSet<String>>();
  /** Maps each node to its strongly connected component. */
  private HashMap<String, HashSet<String>> nodeComponentMap;
  /** Contains the set of IDB predicates that are used recursively in this program. */
  private HashSet<String> recursiveIDBs = null;
  /** An ordered list of components in the (an) order they should be processed. */
  private ArrayList<HashSet<String>> componentProcessingOrder = null;

  /** A helper object used for findSCCs. */
  private Stack<String> visitedNodes;
  /** A helper object used for findSCCs. */
  private HashMap<String, Integer> nodeIndex;
  /** A helper object used for findSCCs. */
  private HashMap<String, Integer> nodeLowLink;
  /** The set of strongly connected components in the dependency graph. */
  private HashSet<HashSet<String>> connectedComponents;
  /** A helper object used in computeProcessingOrder. */
  private HashSet<HashSet<String>> visitedComponents;

  /**
   * Construct and print the dependency graph for the given Datalog program.
   * 
   * @param program the Datalog program to be analyzed.
   */
  public DependencyGraph(final DatalogProgram program) {

    this.program = program;
    this.edbs = program.getProgramBackendNames();

    /* Create the dependency graph. */
    createDependencyGraph();

    /* Find the strongly connected components of the dependency graph. */
    findSCCs();

    /* Find the recursive IDBs. */
    findRecursiveIDBs();
    System.out.println("Recursive IDBs: ");
    System.out.println(recursiveIDBs.toString());

    // computes the processing order based on the dependencies
    this.computeProcessingOrder();
    System.out.println("Processing Order: ");
    System.out.println(componentProcessingOrder.toString());
  }

  /**
   * @param idb the name of an IDB predicate.
   * @return true if the specified predicate is used recursively.
   */
  public boolean isRecursive(final String idb) {

    return recursiveIDBs.contains(idb);
  }

  /**
   * Creates the dependency graph.
   */
  private void createDependencyGraph() {

    /* Loop through each predicate's rules, finding all predicates that it depends on. */
    for (DatalogPredicate currentRule : program.getRuleSet().getPredicates()) {
      /* Will contain a linked hash set of the predicates that this predicate depends on. */
      final LinkedHashSet<String> ruleDependencies = new LinkedHashSet<String>();

      /* Loop through each rule in the definition of this predicate. */
      for (DatalogRule rule : currentRule.getDefiningRules()) {
        /* Add each of the body atoms to the dependencies. */
        ruleDependencies.addAll(rule.getBodyAtomNames());
      }
      depGraph.put(currentRule.getPredicateName(), ruleDependencies);
    }
  }

  /**
   * Implements Tarjan's algorithm for Strongly connected components. This implementation is copied directly from
   * pseudocode on Wikipedia: http://en.wikipedia.org/wiki/Tarjan's_strongly_connected_components_algorithm, still there
   * as of 2012-10-09.
   */
  private void findSCCs() {

    int index = 0;
    visitedNodes = new Stack<String>();
    nodeIndex = new HashMap<String, Integer>();
    nodeLowLink = new HashMap<String, Integer>();
    connectedComponents = new HashSet<HashSet<String>>();
    recursiveIDBs = new HashSet<String>();

    /* This does the main work of the algorithm using the helper function scc */
    for (String pred : depGraph.keySet()) {
      if (!nodeIndex.containsKey(pred)) {
        index = scc(pred, index);
      }
    }
  }

  /**
   * We now have a list of strongly connected components. Figure out which IDBs are accessed recursively.
   * 
   * Must be called after findSCCs.
   */
  private void findRecursiveIDBs() {
    /* Loop through all components. */
    for (HashSet<String> component : connectedComponents) {
      /* If the component has 2 or more nodes, all the IDBs in it are recursive. */
      if (component.size() > 1) {
        this.recursiveIDBs.addAll(component);
        continue;
      }

      /* Component of size 1, see if it recurses on itself. */
      for (String node : component) {
        if (depGraph.get(node).contains(node)) {
          this.recursiveIDBs.add(node);
        }
      }
    }

    /* Create the nodeComponentMap data structure */
    nodeComponentMap = new HashMap<String, HashSet<String>>();
    for (HashSet<String> component : connectedComponents) {
      for (String node : component) {
        nodeComponentMap.put(node, component);
      }
    }
  }

  /**
   * The helper function <tt>strongconnect</tt> defined in the Wikipedia listing referred by findScc.
   * 
   * @param currentNode name of the current predicate.
   * @param index the current depth of the search.
   * @return the depth of the search after recursion, i.e., the total number of nodes visited so far.
   */
  private int scc(final String currentNode, final int index) {

    /* Set the depth index for currentNode to the smallest unused index */
    nodeIndex.put(currentNode, index);
    nodeLowLink.put(currentNode, index);
    int newIndex = index + 1;
    visitedNodes.push(currentNode);

    /* Consider successors of currentNode */
    for (String successorNode : depGraph.get(currentNode)) {
      /* Skip EDBs. (TODO Why? They will always be sinks.) */
      if (edbs.contains(successorNode)) {
        continue;
      }

      if (!nodeIndex.containsKey(successorNode)) {
        /* Successor successorNode has not yet been visited; recurse on it */
        newIndex = scc(successorNode, newIndex);
        nodeLowLink.put(currentNode, Math.min(nodeLowLink.get(currentNode), nodeLowLink.get(successorNode)));
      } else if (visitedNodes.contains(successorNode)) {
        /* Successor successorNode is in the stack and hence in the current SCC */
        nodeLowLink.put(currentNode, Math.min(nodeLowLink.get(currentNode), nodeIndex.get(successorNode)));
      }
    }

    /* If v is a root node, pop the stack and generate a SCC */
    if (nodeLowLink.get(currentNode) == index) {
      final HashSet<String> newComponent = new HashSet<String>();
      String w;
      do {
        w = visitedNodes.pop();
        newComponent.add(w);
      } while (!w.equalsIgnoreCase(currentNode));
      connectedComponents.add(newComponent);
    }

    /* Return the total number of nodes found so far in this search */
    return newIndex;
  }

  /**
   * Computes the processing order of the nodes, as a list of connected components stored at componentProcessingOrder.
   */
  private void computeProcessingOrder() {

    componentProcessingOrder = new ArrayList<HashSet<String>>();
    visitedComponents = new HashSet<HashSet<String>>();

    for (HashSet<String> component : connectedComponents) {
      visitSCC(component);
    }
  }

  /**
   * A helper function for computeProcessingOrder.
   * 
   * @param component the component being explored.
   */
  private void visitSCC(final HashSet<String> component) {

    if ((visitedComponents.contains(component)) || (component == null)) {
      return;
    }
    visitedComponents.add(component);

    for (String node : component) {
      for (String dependency : depGraph.get(node)) {
        visitSCC(nodeComponentMap.get(dependency));
      }
    }
    componentProcessingOrder.add(component);
  }

  /**
   * @return An ordered list of strongly connected components that defines the order in which the components of the
   *         graph should be processed. Each component is represented as a set of node names.
   */
  public ArrayList<HashSet<String>> getProcessingOrder() {

    return this.componentProcessingOrder;
  }

  @Override
  public String toString() {
    String res = "DepGraph:\n";
    res = res + "RuleDeps:\n";
    for (final Iterator<String> it = depGraph.keySet().iterator(); it.hasNext();) {
      final String rName = it.next();
      final Set<String> deps = depGraph.get(rName);
      res = res + rName + ": " + deps.toString() + "\n";
    }
    return res;
  }

}
