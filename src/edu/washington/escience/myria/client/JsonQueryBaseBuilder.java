package edu.washington.escience.myria.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.AbstractConsumerEncoding;
import edu.washington.escience.myria.api.encoding.AbstractProducerEncoding;
import edu.washington.escience.myria.api.encoding.AggregateEncoding;
import edu.washington.escience.myria.api.encoding.BroadcastConsumerEncoding;
import edu.washington.escience.myria.api.encoding.BroadcastProducerEncoding;
import edu.washington.escience.myria.api.encoding.CollectConsumerEncoding;
import edu.washington.escience.myria.api.encoding.CollectProducerEncoding;
import edu.washington.escience.myria.api.encoding.ColumnSelectEncoding;
import edu.washington.escience.myria.api.encoding.ConsumerEncoding;
import edu.washington.escience.myria.api.encoding.DbInsertEncoding;
import edu.washington.escience.myria.api.encoding.DupElimEncoding;
import edu.washington.escience.myria.api.encoding.DupElimStateEncoding;
import edu.washington.escience.myria.api.encoding.EOSControllerEncoding;
import edu.washington.escience.myria.api.encoding.FileScanEncoding;
import edu.washington.escience.myria.api.encoding.FilterEncoding;
import edu.washington.escience.myria.api.encoding.IDBControllerEncoding;
import edu.washington.escience.myria.api.encoding.LocalMultiwayConsumerEncoding;
import edu.washington.escience.myria.api.encoding.LocalMultiwayProducerEncoding;
import edu.washington.escience.myria.api.encoding.OperatorEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryScanEncoding;
import edu.washington.escience.myria.api.encoding.ShuffleConsumerEncoding;
import edu.washington.escience.myria.api.encoding.ShuffleProducerEncoding;
import edu.washington.escience.myria.api.encoding.SingleGroupByAggregateEncoding;
import edu.washington.escience.myria.api.encoding.SinkRootEncoding;
import edu.washington.escience.myria.api.encoding.StreamingStateEncoding;
import edu.washington.escience.myria.api.encoding.SymmetricHashJoinEncoding;
import edu.washington.escience.myria.api.encoding.TableScanEncoding;
import edu.washington.escience.myria.api.encoding.TipsyFileScanEncoding;
import edu.washington.escience.myria.api.encoding.UnionAllEncoding;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.operator.ColumnSelect;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TipsyFileScan;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.Aggregator;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myria.parallel.LocalMultiwayProducer;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Json query builder base implementation. This class provides spark-like query plan building functionality.
 * 
 * */
public class JsonQueryBaseBuilder implements JsonQueryBuilder {

  /**
   * Shared data among all instances in a single building process.
   * */
  private static final class SharedData {

    /**
     * globalWorkers all workers in the computing system.
     */
    private Set<Integer> globalWorkers;

    /**
     * record all operators this builder built.
     * */
    private final Set<JsonQueryBaseBuilder> allOperators;

    /**
     * User defined operator name to operator mapping.
     * */
    private final Map<String, JsonQueryBaseBuilder> userDefinedName2OpMap;

    /**
     * operator to user defined operator name mapping.
     * */
    private final Map<JsonQueryBaseBuilder, String> op2UserDefinedNameMap;

    /**
     * Random number generator.
     * */
    private final Random rand = new Random();

    /**
     * Constructor.
     * */
    private SharedData() {
      allOperators = new HashSet<JsonQueryBaseBuilder>();
      userDefinedName2OpMap = new HashMap<String, JsonQueryBaseBuilder>();
      op2UserDefinedNameMap = new HashMap<JsonQueryBaseBuilder, String>();
      globalWorkers = NO_PREFERENCE;
    }

  }

  /**
   * Shared data.
   * */
  private final SharedData sharedData;

  /**
   * The current {@link Operator}, or the output of the current {@link Operator}, this builder represents.
   * */
  private final OperatorEncoding<?> op;
  /**
   * The children of the current op.
   * */
  private final JsonQueryBaseBuilder[] children;
  /**
   * The field names which are the children of the current op.
   * */
  private final String[] childrenFields;
  /**
   * The workers on which the current operator is going to run, i.e. the operator partition.
   * */
  private final Set<Integer> runOnWorkers;
  /**
   * An operator is going to run on any single worker.
   * */
  private static final Set<Integer> ANY_SINGLE_WORKER = Collections.unmodifiableSet(new HashSet<Integer>(0));
  /**
   * An operator is going to run on all workers.
   * */
  private static final Set<Integer> ALL_WORKERS = Collections.unmodifiableSet(new HashSet<Integer>(0));
  /**
   * An operator is going to run on any worker settings. The actual setting is decided by either other operators which
   * are in the same task as this operator, for example if the operator is in the same task as a collect consumer the
   * operator will be running on the same single worker as the collect consumer, or by the system.
   */
  private static final Set<Integer> NO_PREFERENCE = Collections.unmodifiableSet(new HashSet<Integer>(0));
  /**
   * The parents of the current op. It is for automatically generating {@link LocalMultiwayProducer} and
   * {@link LocalMultiwayConsumer} pairs.
   * */
  private final Set<JsonQueryBaseBuilder> parents;

  /**
   * Operator name prefix. For use in automatically constructing operator names.
   * */
  public static final Map<Class<? extends OperatorEncoding<?>>, String> OPERATOR_PREFICES =
      new HashMap<Class<? extends OperatorEncoding<?>>, String>();
  static {
    OPERATOR_PREFICES.put(LocalMultiwayProducerEncoding.class, "lmp");
    OPERATOR_PREFICES.put(LocalMultiwayConsumerEncoding.class, "lmc");
    OPERATOR_PREFICES.put(ShuffleProducerEncoding.class, "sp");
    OPERATOR_PREFICES.put(ShuffleConsumerEncoding.class, "sc");
    OPERATOR_PREFICES.put(SymmetricHashJoinEncoding.class, "join");
    OPERATOR_PREFICES.put(CollectProducerEncoding.class, "cp");
    OPERATOR_PREFICES.put(CollectConsumerEncoding.class, "cc");
    OPERATOR_PREFICES.put(BroadcastProducerEncoding.class, "bp");
    OPERATOR_PREFICES.put(BroadcastConsumerEncoding.class, "bc");
    OPERATOR_PREFICES.put(SinkRootEncoding.class, "sink");
    OPERATOR_PREFICES.put(DbInsertEncoding.class, "insert");
    OPERATOR_PREFICES.put(IDBControllerEncoding.class, "idbinput");
    OPERATOR_PREFICES.put(EOSControllerEncoding.class, "eosController");
    OPERATOR_PREFICES.put(TipsyFileScanEncoding.class, "tipsy");
    OPERATOR_PREFICES.put(FileScanEncoding.class, "file");
    OPERATOR_PREFICES.put(FilterEncoding.class, "filter");
    OPERATOR_PREFICES.put(ColumnSelectEncoding.class, "project");
  }

  /**
   * Worker set algebra. Given two worker set, compute the compatible worker set.
   * 
   * @param workers1 worker set 1
   * @param workers2 worker set 2
   * @return compatible worker set.
   * */
  private static Set<Integer> workerSetAlgebra(@Nonnull final Set<Integer> workers1,
      @Nonnull final Set<Integer> workers2) {
    Preconditions.checkNotNull(workers1);
    Preconditions.checkNotNull(workers2);

    if (workers1 == ANY_SINGLE_WORKER) {
      if (workers2 == ANY_SINGLE_WORKER || workers2 == NO_PREFERENCE) {
        return ANY_SINGLE_WORKER;
      } else if (workers2 == ALL_WORKERS) {
        return null;
      } else {
        if (workers2.size() != 1) {
          return null;
        } else {
          return workers2;
        }
      }
    } else if (workers1 == ALL_WORKERS) {
      if (workers2 == ANY_SINGLE_WORKER) {
        return null;
      } else if (workers2 == ALL_WORKERS || workers2 == NO_PREFERENCE) {
        return ALL_WORKERS;
      } else {
        return null;
      }
    } else if (workers1 == NO_PREFERENCE) {
      return workers2;
    } else {
      if (workers2 == ANY_SINGLE_WORKER) {
        if (workers1.size() == 1 && !workers1.contains(MyriaConstants.MASTER_ID)) {
          return workers1;
        } else {
          return null;
        }
      } else if (workers2 == ALL_WORKERS) {
        return null;
      } else if (workers2 == NO_PREFERENCE) {
        return workers1;
      } else {
        if (workers1.size() != workers2.size()) {
          return null;
        }
        for (Integer w : workers1) {
          if (!workers2.contains(w)) {
            return null;
          }
        }
        return workers1;
      }
    }
  }

  /**
   * copy constructor.
   * 
   * @param currentOp current operator to be wrapped in the new created {@link JsonQueryBaseBuilder}
   * @param childrenFields children fields in current op
   * @param children the children
   * @param runningWorkers the worker set to run on
   * @param sharedData shared data.
   * @param compatibleWithChildrenWorkers if true, make sure the current op's worker set is compatible with the
   *          children's, else no check, which happens in Producer/Consumer pairs
   * */
  private JsonQueryBaseBuilder(final OperatorEncoding<?> currentOp, final String[] childrenFields,
      final JsonQueryBaseBuilder[] children, @Nonnull final Set<Integer> runningWorkers,
      final boolean compatibleWithChildrenWorkers, final SharedData sharedData) {
    op = currentOp;
    this.sharedData = sharedData;

    if (children == null || children.length == 0) {
      this.children = new JsonQueryBaseBuilder[] {};
    } else {
      this.children = children;
      for (final JsonQueryBaseBuilder c : children) {
        if (sharedData != c.sharedData) {
          throw new IllegalArgumentException("Cannot combine operators built by different builders.");
        }
        c.parents.add(this);
      }

    }

    this.childrenFields = childrenFields;

    parents = new HashSet<JsonQueryBaseBuilder>();

    Set<Integer> childrenWorkers = NO_PREFERENCE;
    for (final JsonQueryBaseBuilder c : children) {
      childrenWorkers = workerSetAlgebra(childrenWorkers, c.runOnWorkers);
      if (childrenWorkers == null) {
        throw new IllegalArgumentException("Workers of a child are not compatible with other children. Current op: "
            + getOpName(this) + ", conflicting child: " + getOpName(c));
      }
    }
    if (compatibleWithChildrenWorkers) {
      runOnWorkers = workerSetAlgebra(childrenWorkers, runningWorkers);
      if (runOnWorkers == null) {
        String[] childrenNames = new String[children.length];
        for (int i = 0; i < children.length; i++) {
          childrenNames[i] = getOpName(children[i]);
        }
        throw new IllegalArgumentException("Running workers are not compatible with children workers. Current op: "
            + getOpName(this) + ", children: " + StringUtils.join(childrenNames, ','));
      }
    } else {
      runOnWorkers = runningWorkers;
    }
  }

  /**
   * Constructor.
   * 
   * */
  public JsonQueryBaseBuilder() {
    op = null;
    children = new JsonQueryBaseBuilder[] {};
    childrenFields = new String[] {};
    runOnWorkers = new HashSet<Integer>();
    parents = new HashSet<JsonQueryBaseBuilder>();
    sharedData = new SharedData();
  }

  /**
   * Check if an operator is a root.
   * 
   * @param op the operator to check
   * @return the check result.
   * */
  private boolean isRootOp(final JsonQueryBaseBuilder op) {
    if (op.op instanceof AbstractProducerEncoding || op.op instanceof DbInsertEncoding
        || op.op instanceof SinkRootEncoding) {
      return true;
    }
    return false;
  }

  /**
   * Check if the operator is a root operator without output streams. For example {@link SinkRoot}.
   * 
   * @return the check result.
   * @param op the op to check.
   * */
  private static boolean isSinkRootOp(final JsonQueryBaseBuilder op) {
    if (op.op instanceof DbInsertEncoding || op.op instanceof SinkRootEncoding) {
      return true;
    }
    return false;
  }

  /**
   * Fork the output stream of an operator by adding {@link LocalMultiwayProducer}/{@link LocalMultiwayConsumer} pair.
   * 
   * @param toMulti the operator to be forked
   * */
  private void insertLocalMultiway(final JsonQueryBaseBuilder toMulti) {
    if (toMulti.op instanceof LocalMultiwayProducerEncoding) {
      return;
    }
    JsonQueryBaseBuilder[] oldParents = toMulti.parents.toArray(new JsonQueryBaseBuilder[] {});
    JsonQueryBaseBuilder lP = buildOperator(LocalMultiwayProducerEncoding.class, "argChild", toMulti, NO_PREFERENCE);
    for (JsonQueryBaseBuilder parent : oldParents) {
      JsonQueryBaseBuilder lC = buildOperator(LocalMultiwayConsumerEncoding.class, "argOperatorId", lP, NO_PREFERENCE);
      for (int i = 0; i < parent.children.length; i++) {
        JsonQueryBaseBuilder child = parent.children[i];
        if (child == toMulti) {
          parent.children[i] = lC;
          break;
        }
      }
    }
    toMulti.parents.clear();
    toMulti.parents.add(lP);
  }

  /**
   * Process output stream forking.
   * */
  private void processLocalStreamForks() {
    for (JsonQueryBaseBuilder opp : sharedData.allOperators.toArray(new JsonQueryBaseBuilder[] {})) {
      // add local multiway producers
      if (opp.parents.size() > 1) {
        insertLocalMultiway(opp);
      }
    }
  }

  /**
   * Process iterations. This method will add the {@link IDBInputEncoding}, {@link EOSControllerEncoding} and other
   * related operators into the query plan if the query plan contains iterations.
   * */
  private void processIterations() {

    HashSet<JsonQueryBaseBuilder> iterationNodes = new HashSet<JsonQueryBaseBuilder>();
    for (JsonQueryBaseBuilder opp : sharedData.allOperators) {
      if (opp.op instanceof IterateEndPlaceHolder) {
        iterationNodes.add(opp);
      }
    }

    if (iterationNodes.size() <= 0) {
      return;
    } else {

      JsonQueryBaseBuilder[] iterationEndPoints = iterationNodes.toArray(new JsonQueryBaseBuilder[] {});

      JsonQueryBaseBuilder[] eoiReceivers = new JsonQueryBaseBuilder[iterationEndPoints.length];
      for (int i = 0; i < iterationEndPoints.length; i++) {
        JsonQueryBaseBuilder fakeScan = buildOperator(QueryScanEncoding.class, NO_PREFERENCE);
        eoiReceivers[i] =
            buildOperator(ConsumerEncoding.class, new String[] { "argOperatorId" },
                new JsonQueryBaseBuilder[] { fakeScan }, NO_PREFERENCE);

        eoiReceivers[i].setName("eoiReceiver#" + i);
        eoiReceivers[i].op.opID = "eoiReceiver#" + i;
        sharedData.allOperators.add(eoiReceivers[i]);

      }

      JsonQueryBaseBuilder eoiInput = buildOperator(UnionAllEncoding.class, "argChildren", eoiReceivers, NO_PREFERENCE);
      eoiInput.setName("allEOIReports");
      eoiInput.op.opID = "allEOIReports";

      JsonQueryBaseBuilder eosC = buildOperator(EOSControllerEncoding.class, "argChild", eoiInput, ANY_SINGLE_WORKER);
      eosC.setName("eosController");
      eosC.op.opID = "eosController";

      for (int i = 0; i < iterationEndPoints.length; i++) {
        JsonQueryBaseBuilder iterationEndPoint = iterationEndPoints[i];
        JsonQueryBaseBuilder iterationBeginPoint = ((IterateEndPlaceHolder) iterationEndPoint.op).iterationBeginPoint;
        JsonQueryBaseBuilder initialInput = iterationBeginPoint.children[0];
        JsonQueryBaseBuilder iterationInput = iterationEndPoint.children[0];

        JsonQueryBaseBuilder iterationBeginParent = iterationBeginPoint.parents.iterator().next();

        initialInput.parents.remove(iterationBeginPoint);
        iterationInput.parents.remove(iterationEndPoint);

        sharedData.allOperators.remove(iterationBeginPoint);
        sharedData.allOperators.remove(iterationEndPoint);

        JsonQueryBaseBuilder eosReceiver =
            buildOperator(ConsumerEncoding.class, new String[] { "argOperatorId" },
                new JsonQueryBaseBuilder[] { eosC }, NO_PREFERENCE);
        eosReceiver.setName("eosReceiver#" + i);

        JsonQueryBaseBuilder idbC =
            buildOperator(IDBControllerEncoding.class, new String[] {
                "argInitialInput", "argIterationInput", "argEosControllerInput" }, new JsonQueryBaseBuilder[] {
                initialInput, iterationInput, eosReceiver }, NO_PREFERENCE);
        idbC.op.opID = "idbInput#" + i;
        ((IDBControllerEncoding) idbC.op).argSelfIdbId = i;
        ((IDBControllerEncoding) idbC.op).argState =
            ((IterateBeginPlaceHolder) (iterationBeginPoint.op)).idbStateProcessor;

        for (int j = 0; j < iterationBeginParent.children.length; j++) {
          JsonQueryBaseBuilder c = iterationBeginParent.children[j];
          if (c == iterationBeginPoint) {
            iterationBeginParent.children[j] = idbC;
          }
        }

        idbC.parents.add(iterationBeginParent);

        eoiReceivers[i].children[0] = idbC;

      }
    }
  }

  @Override
  public final JsonQueryBaseBuilder workers(final int[] ws) {
    sharedData.globalWorkers = MyriaArrayUtils.checkSet(ArrayUtils.toObject(ws));
    return this;
  }

  /**
   * @param b the builder.
   * @return the operator name.
   * */
  private String getOpName(final JsonQueryBaseBuilder b) {
    if (b.op.opID != null) {
      return b.op.opID;
    }
    for (JsonQueryBaseBuilder opp : sharedData.allOperators) {
      setOpNames(opp);
    }
    return b.op.opID;
  }

  /**
   * @return the encoding of the query this builder is going to build.
   * */
  @Override
  public final QueryEncoding build() {
    if (!isRootOp(this)) {
      // add a SinkRoot if the current op is not a root op.
      return buildOperator(SinkRootEncoding.class, "argChild", this, NO_PREFERENCE).build();
    } else {

      processLocalStreamForks();
      processIterations();

      final QueryEncoding result = new QueryEncoding();

      for (JsonQueryBaseBuilder opp : sharedData.allOperators) {
        setOpNames(opp);
      }

      for (JsonQueryBaseBuilder opp : sharedData.allOperators) {
        updateChildrenFields(opp);
      }

      Map<JsonQueryBaseBuilder, PlanFragmentEncoding> fragments =
          new HashMap<JsonQueryBaseBuilder, PlanFragmentEncoding>();
      for (JsonQueryBaseBuilder opp : sharedData.allOperators) {
        if (isRootOp(opp)) {
          buildFragment(fragments, opp);
        }
      }

      result.fragments = Arrays.asList(fragments.values().toArray(new PlanFragmentEncoding[] {}));
      result.ftMode = MyriaConstants.FTMODE.none.name();
      result.logicalRa = "";
      result.profilingMode = false;
      result.rawDatalog = "";

      try {
        result.validate();
      } catch (MyriaApiException e) {
        throw new IllegalArgumentException("Invalid query built by: " + this.getClass().getCanonicalName()
            + ". Cause: " + e.getResponse().getEntity().toString(), e.getCause());
      }

      return result;
    }
  }

  /**
   * @return the json format of the query built.
   * */
  @Override
  public final String buildJson() {
    ObjectMapper ow = MyriaJsonMapperProvider.getMapper();

    QueryEncoding qe = build();
    try {
      return ow.writerWithDefaultPrettyPrinter().writeValueAsString(qe);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Build a single child {@link JsonQueryBaseBuilder}.
   * 
   * @param operatorClass the class of the wrapping operator.
   * @param <T> the type of the operator.
   * @param childField the child field name
   * @param child the child
   * @param runningWorkers worker set.
   * @return a wrapping {@link JsonQueryBaseBuilder}
   * @throws IllegalArgumentException if any argument is illegal
   * */
  private <T extends OperatorEncoding<?>> JsonQueryBaseBuilder buildOperator(final Class<T> operatorClass,
      final String childField, final JsonQueryBaseBuilder child, final Set<Integer> runningWorkers)
      throws IllegalArgumentException {
    return buildOperator(Preconditions.checkNotNull(operatorClass), new String[] { Preconditions
        .checkNotNull(childField) }, new JsonQueryBaseBuilder[] { Preconditions.checkNotNull(child) }, runningWorkers);
  }

  /**
   * Build a leaf {@link JsonQueryBaseBuilder}.
   * 
   * @param operatorClass the class of the wrapping operator.
   * @param <T> the type of the operator.
   * @param runningWorkers worker set.
   * @return a wrapping {@link JsonQueryBaseBuilder}
   * @throws IllegalArgumentException if any argument is illegal
   * */
  private <T extends OperatorEncoding<?>> JsonQueryBaseBuilder buildOperator(final Class<T> operatorClass,
      final Set<Integer> runningWorkers) throws IllegalArgumentException {
    return buildOperator(Preconditions.checkNotNull(operatorClass), new String[] {}, new JsonQueryBaseBuilder[] {},
        runningWorkers);
  }

  /**
   * Build a multi-children {@link JsonQueryBaseBuilder}. And the children are set in a single array field.
   * 
   * @param operatorClass the class of the wrapping operator.
   * @param <T> the type of the operator.
   * @param childrenField the children array field name
   * @param children the children
   * @param runningWorkers worker set.
   * @return a wrapping {@link JsonQueryBaseBuilder}
   * @throws IllegalArgumentException if any argument is illegal
   * */
  private <T extends OperatorEncoding<?>> JsonQueryBaseBuilder buildOperator(final Class<T> operatorClass,
      final String childrenField, final JsonQueryBaseBuilder[] children, final Set<Integer> runningWorkers)
      throws IllegalArgumentException {
    return buildOperator(Preconditions.checkNotNull(operatorClass), new String[] { Preconditions
        .checkNotNull(childrenField) }, Preconditions.checkNotNull(children), runningWorkers);
  }

  /**
   * Build a {@link JsonQueryBaseBuilder} wrapping an op.
   * 
   * @param operatorClass the java encoding class of the operator to get built.
   * @param <T> the type of the operator.
   * @param childrenFields the children field names
   * @param children the children
   * @param runningWorkers worker set.
   * @return a wrapping {@link JsonQueryBaseBuilder}
   * @throws IllegalArgumentException if any argument is illegal
   * */
  private <T extends OperatorEncoding<?>> JsonQueryBaseBuilder buildOperator(@Nonnull final Class<T> operatorClass,
      @Nonnull final String[] childrenFields, @Nonnull final JsonQueryBaseBuilder[] children,
      @Nonnull final Set<Integer> runningWorkers) throws IllegalArgumentException {
    for (JsonQueryBaseBuilder ch : children) {
      if (isSinkRootOp(ch)) {
        throw new IllegalArgumentException(
            "Root Op without output datastreams cannot serve as children to other operators: "
                + ch.op.getClass().getSimpleName() + " is set as child of: " + operatorClass.getSimpleName());
      }
    }

    try {
      T newOp = operatorClass.newInstance();
      JsonQueryBaseBuilder newOpB =
          new JsonQueryBaseBuilder(newOp, childrenFields, children, runningWorkers, !AbstractConsumerEncoding.class
              .isAssignableFrom(operatorClass), sharedData);
      newOpB.sharedData.allOperators.add(newOpB);
      return newOpB;
    } catch (InstantiationException | IllegalAccessException e) {
      e.printStackTrace();
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * @param root the root of the fragment
   * @param fragments the current built fragments.
   * */
  private void buildFragment(final Map<JsonQueryBaseBuilder, PlanFragmentEncoding> fragments,
      final JsonQueryBaseBuilder root) {
    if (fragments.get(root) != null) {
      return;
    }
    Preconditions.checkNotNull(root.runOnWorkers);

    ArrayList<JsonQueryBaseBuilder> operators = new ArrayList<JsonQueryBaseBuilder>();
    PlanFragmentEncoding fragment = new PlanFragmentEncoding();
    findOperators(operators, root);
    fragment.operators = new ArrayList<OperatorEncoding<?>>(operators.size());

    for (JsonQueryBaseBuilder obb : operators) {
      fragment.operators.add(obb.op);
    }

    if (root.runOnWorkers == ANY_SINGLE_WORKER) {
      Preconditions.checkArgument(sharedData.globalWorkers.size() > 0,
          "A set of workers must be provided if a query contains a fragment which must be run on a single worker.");
      fragment.workers =
          Arrays.asList(new Integer[] { sharedData.globalWorkers.toArray(new Integer[] {})[sharedData.rand
              .nextInt(sharedData.globalWorkers.size())] });
    } else if (root.runOnWorkers == ALL_WORKERS || root.runOnWorkers == NO_PREFERENCE) {
      fragment.workers = null;
    } else if (root.runOnWorkers.size() <= 0) {
      throw new IllegalArgumentException("Number of workers of a fragment must be positive. Root: " + getOpName(root)
          + ". " + root.runOnWorkers.getClass());
    } else {
      fragment.workers = Arrays.asList(root.runOnWorkers.toArray(new Integer[] {}));
    }

    fragments.put(root, fragment);
  }

  /**
   * Update the children fields for the given operator. This method is called after the names of all the operators are
   * set.
   * 
   * @param currentOp the operator to update.
   * */
  private void updateChildrenFields(final JsonQueryBaseBuilder currentOp) {
    // update children field names
    String[] childrenNames = new String[currentOp.children.length];
    int idx = 0;
    for (JsonQueryBaseBuilder c : currentOp.children) {
      childrenNames[idx++] = c.op.opID;
    }

    if (currentOp.childrenFields.length == 1) {
      try {
        Field childrenField = currentOp.op.getClass().getField(currentOp.childrenFields[0]);
        if (childrenField.getType().equals(String.class)) {
          childrenField.set(currentOp.op, childrenNames[0]);
        } else if (childrenField.getType().equals(String[].class)) {
          childrenField.set(currentOp.op, childrenNames);
        }
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    } else {
      try {
        int i = 0;
        for (String childFieldName : currentOp.childrenFields) {
          Field childField = currentOp.op.getClass().getField(childFieldName);
          childField.set(currentOp.op, childrenNames[i++]);
        }
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Set all the operator names rooted by a given operator.
   * 
   * @param root the root operator to start
   * */
  private void setOpNames(final JsonQueryBaseBuilder root) {
    setOpNames(root, new HashMap<String, JsonQueryBaseBuilder>());
  }

  /**
   * Set all the operator names rooted by a given operator.
   * 
   * @param root the root operator to start
   * @param namedOperators record the name to op mapping.
   * */
  private void setOpNames(final JsonQueryBaseBuilder root, final HashMap<String, JsonQueryBaseBuilder> namedOperators) {
    if (root.op.opID != null) {
      return;
    } else {
      String opName = sharedData.op2UserDefinedNameMap.get(root);
      if (opName != null) {
        root.op.opID = opName;
        return;
      } else {
        for (JsonQueryBaseBuilder f : root.children) {
          setOpNames(f, namedOperators);
        }
        buildOpNameFromChildren(root, namedOperators);
      }
    }
  }

  /**
   * Build the name of an operator if not specified by user.
   * 
   * @param currentOp the operator which is to get name built.
   * @param namedOperators the operator name -> operator mapping.
   * */
  private void buildOpNameFromChildren(final JsonQueryBaseBuilder currentOp,
      final HashMap<String, JsonQueryBaseBuilder> namedOperators) {

    String[] childrenNames = new String[currentOp.children.length];
    int idx = 0;
    for (JsonQueryBaseBuilder c : currentOp.children) {
      childrenNames[idx++] = c.op.opID;
    }

    String namePrefix = null;
    namePrefix = OPERATOR_PREFICES.get(currentOp.op.getClass());

    if (namePrefix == null) {
      namePrefix = currentOp.op.getClass().getSimpleName();
    }

    if (namePrefix.endsWith("Encoding")) {
      namePrefix = namePrefix.substring(0, namePrefix.length() - "Encoding".length());
    }
    StringBuilder opNameBuilder = new StringBuilder(namePrefix);

    if (currentOp.op instanceof AbstractConsumerEncoding) {
      // for consumers, do not include the child producer's XXXProducer prefix
      opNameBuilder.append('(');
      opNameBuilder.append(currentOp.children[0].children[0].op.opID);
      opNameBuilder.append(')');
    } else {
      if (currentOp.children.length > 0) {
        opNameBuilder.append('(');
        opNameBuilder.append(StringUtils.join(childrenNames, ','));
        opNameBuilder.append(')');
      }
    }

    String name = opNameBuilder.toString();

    String suffix = "";
    int suffixNumber = 0;
    while (namedOperators.containsKey(name + suffix)) {
      suffix = "" + (++suffixNumber);
    }
    name = name + suffix;
    namedOperators.put(name, currentOp);
    currentOp.op.opID = name;

  }

  /**
   * find all the operators that are rooted by the currentRoot operator.
   * 
   * @param operators existing operators
   * @param currentRoot the current root operator
   * */
  private void findOperators(final List<JsonQueryBaseBuilder> operators, final JsonQueryBaseBuilder currentRoot) {
    operators.add(currentRoot);
    if (!(currentRoot.op instanceof AbstractConsumerEncoding)) {
      for (JsonQueryBaseBuilder f : currentRoot.children) {
        findOperators(operators, f);
      }
    }
  }

  /**
   * {@link SymmetricHashJoin}.
   * 
   * @return builder.
   * @param other to join
   * @param myCmpColumns my cmp columns
   * @param otherCmpColumns other cmp columns
   * @param myResultColumns my result columns
   * @param otherResultColumns other result columns
   * */
  public JsonQueryBaseBuilder hashEquiJoin(final JsonQueryBuilder other, final int[] myCmpColumns,
      final int[] myResultColumns, final int[] otherCmpColumns, final int[] otherResultColumns) {

    JsonQueryBaseBuilder jbb =
        buildOperator(SymmetricHashJoinEncoding.class, new String[] { "argChild1", "argChild2" },
            new JsonQueryBaseBuilder[] { this, (JsonQueryBaseBuilder) other }, NO_PREFERENCE);

    SymmetricHashJoinEncoding join = (SymmetricHashJoinEncoding) jbb.op;
    join.argColumns1 = myCmpColumns;
    join.argColumns2 = otherCmpColumns;
    join.argSelect1 = myResultColumns;
    join.argSelect2 = otherResultColumns;
    return jbb;
  }

  /**
   * {@link Filter}.
   * 
   * @return builder.
   * @param predicate predicate.
   */
  public JsonQueryBaseBuilder filter(final Expression predicate) {
    JsonQueryBaseBuilder filter = buildOperator(FilterEncoding.class, NO_PREFERENCE);
    ((FilterEncoding) filter.op).argPredicate = predicate;
    return filter;
  }

  /**
   * Column selection (Project).
   * 
   * {@link ColumnSelect}.
   * 
   * @return builder.
   * @param fieldList list of fields to be remained
   */
  public JsonQueryBaseBuilder project(final int[] fieldList) {
    JsonQueryBaseBuilder project = buildOperator(ColumnSelectEncoding.class, NO_PREFERENCE);
    ((ColumnSelectEncoding) project.op).argFieldList = fieldList;
    return project;
  }

  /**
   * Query scan.
   * 
   * {@link DbQueryScan}.
   * 
   * @return builder.
   * @param sql the sql query
   * @param outputSchema schema
   */
  public JsonQueryBaseBuilder queryScan(final String sql, final Schema outputSchema) {
    JsonQueryBaseBuilder scan = buildOperator(QueryScanEncoding.class, ALL_WORKERS);
    ((QueryScanEncoding) scan.op).sql = sql;
    ((QueryScanEncoding) scan.op).schema = outputSchema;
    return scan;
  }

  /**
   * {@link TipsyFileScan}.
   * 
   * @return builder.
   * @param tipsyFilename .
   * @param iorderFilename .
   * @param grpFilename .
   * @param outputSchema schema
   * */
  public JsonQueryBaseBuilder tipsyScan(final String tipsyFilename, final String iorderFilename,
      final String grpFilename, final Schema outputSchema) {
    JsonQueryBaseBuilder scan = buildOperator(TipsyFileScanEncoding.class, ALL_WORKERS);
    ((TipsyFileScanEncoding) scan.op).grpFilename = grpFilename;
    ((TipsyFileScanEncoding) scan.op).iorderFilename = iorderFilename;
    ((TipsyFileScanEncoding) scan.op).tipsyFilename = tipsyFilename;
    return scan;
  }

  /**
   * {@link FileScan}.
   * 
   * @return builder.
   * @param fileName .
   * @param delimeter .
   * @param outputSchema schema
   * */
  public JsonQueryBaseBuilder fileScan(final String fileName, final Character delimeter, final Schema outputSchema) {
    JsonQueryBaseBuilder scan = buildOperator(FileScanEncoding.class, ALL_WORKERS);
    ((FileScanEncoding) scan.op).delimiter = delimeter;
    ((FileScanEncoding) scan.op).source = new FileSource(fileName);
    ((FileScanEncoding) scan.op).schema = outputSchema;
    return scan;
  }

  /**
   * Table scan.
   * 
   * {@link DbQueryScan}.
   * 
   * @return builder.
   * @param table the table.
   * */
  public JsonQueryBaseBuilder scan(final RelationKey table) {
    Preconditions.checkNotNull(table);
    // generateID("TableScan: " + table),
    JsonQueryBaseBuilder scan = buildOperator(TableScanEncoding.class, ALL_WORKERS);
    ((TableScanEncoding) scan.op).relationKey = table;
    return scan;
  }

  /**
   * {@link ShuffleProducer} {@link ShuffleConsumer} pair.
   * 
   * @return builder.
   * @param pf partition function
   * */
  public JsonQueryBaseBuilder shuffle(final PartitionFunction pf) {
    JsonQueryBaseBuilder shuffleP = buildOperator(ShuffleProducerEncoding.class, "argChild", this, NO_PREFERENCE);
    ((ShuffleProducerEncoding) shuffleP.op).argPf = pf;
    return buildOperator(ShuffleConsumerEncoding.class, "argOperatorId", shuffleP, NO_PREFERENCE);
  }

  /**
   * {@link CollectProducer} {@link CollectConsumer} pair.
   * 
   * @return builder.
   * */
  public JsonQueryBaseBuilder collect() {
    JsonQueryBaseBuilder p = buildOperator(CollectProducerEncoding.class, "argChild", this, NO_PREFERENCE);
    return buildOperator(CollectConsumerEncoding.class, "argOperatorId", p, ANY_SINGLE_WORKER);
  }

  /**
   * {@link CollectProducer} {@link CollectConsumer} pair.
   * 
   * @return builder.
   * @param workerID collect destination.
   * */
  public JsonQueryBaseBuilder collect(final int workerID) {
    JsonQueryBaseBuilder p = buildOperator(CollectProducerEncoding.class, "argChild", this, NO_PREFERENCE);
    JsonQueryBaseBuilder c =
        buildOperator(CollectConsumerEncoding.class, "argOperatorId", p, ImmutableSet.of(workerID));
    return c;
  }

  /**
   * {@link CollectProducer} {@link CollectConsumer} pair, send to master.
   * 
   * @return builder.
   * */
  public JsonQueryBaseBuilder masterCollect() {
    return this.collect(MyriaConstants.MASTER_ID);
  }

  @Override
  public JsonQueryBaseBuilder export() {
    // TODO
    return this.collect(MyriaConstants.MASTER_ID);
  }

  /**
   * {@link BroadcastProducer} and {@link BroadcastConsumer} pair.
   * 
   * @return builder.
   * */
  public JsonQueryBaseBuilder broadcast() {
    JsonQueryBaseBuilder p = buildOperator(BroadcastProducerEncoding.class, "argChild", this, NO_PREFERENCE);
    return buildOperator(BroadcastProducerEncoding.class, "argOperatorId", p, NO_PREFERENCE);
  }

  /**
   * {@link DupElim}.
   * 
   * @return builder.
   * */
  public JsonQueryBaseBuilder dupElim() {
    return buildOperator(DupElimEncoding.class, "argChild", this, NO_PREFERENCE);
  }

  /**
   * {@link UnionAll}.
   * 
   * @return builder.
   * @param others others.
   * */
  public JsonQueryBaseBuilder union(final Set<JsonQueryBuilder> others) {
    JsonQueryBaseBuilder[] childrenL = others.toArray(new JsonQueryBaseBuilder[others.size() + 1]);
    childrenL[childrenL.length - 1] = this;
    return buildOperator(UnionAllEncoding.class, "argChildren", childrenL, NO_PREFERENCE);
  }

  /**
   * {@link UnionAll}.
   * 
   * @return builder.
   * @param other other.
   * */
  public JsonQueryBaseBuilder union(final JsonQueryBuilder other) {
    return buildOperator(UnionAllEncoding.class, "argChildren", new JsonQueryBaseBuilder[] {
        this, (JsonQueryBaseBuilder) other }, NO_PREFERENCE);
  }

  /**
   * {@link Aggregate}.
   * 
   * @return builder.
   * @param aggColumns agg columns
   * @param aggOps agg ops.
   * */
  public JsonQueryBaseBuilder aggregate(final int[] aggColumns, final int[] aggOps) {
    List<List<String>> ops = AggregateEncoding.serializeAggregateOperator(aggOps);
    JsonQueryBaseBuilder agg = buildOperator(AggregateEncoding.class, "argChild", this, NO_PREFERENCE);
    ((AggregateEncoding) agg.op).argAggFields = aggColumns;
    ((AggregateEncoding) agg.op).argAggOperators = ops;
    return agg;
  }

  /**
   * {@link SymmetricHashJoin}.
   * 
   * @return builder.
   * @param groupColumn group by column
   * @param aggColumns agg columns
   * @param aggOps agg ops.
   * */
  public JsonQueryBaseBuilder groupBy(final int groupColumn, final int[] aggColumns, final int[] aggOps) {
    List<List<String>> ops = AggregateEncoding.serializeAggregateOperator(aggOps);
    JsonQueryBaseBuilder gp = buildOperator(SingleGroupByAggregateEncoding.class, "argChild", this, NO_PREFERENCE);
    ((SingleGroupByAggregateEncoding) gp.op).argAggFields = aggColumns;
    ((SingleGroupByAggregateEncoding) gp.op).argAggOperators = ops;
    ((SingleGroupByAggregateEncoding) gp.op).argGroupField = groupColumn;
    return gp;
  }

  /**
   * {@link Insert}.
   * 
   * @return builder.
   * @param relationKey .
   * */
  public JsonQueryBaseBuilder dbInsert(final RelationKey relationKey) {
    JsonQueryBaseBuilder insertJBB = buildOperator(DbInsertEncoding.class, "argChild", this, NO_PREFERENCE);
    DbInsertEncoding insert = (DbInsertEncoding) insertJBB.op;
    insert.argOverwriteTable = true;
    insert.connectionInfo = null;
    insert.relationKey = relationKey;
    return insertJBB;
  }

  @Override
  public final JsonQueryBaseBuilder setName(final String name) {
    JsonQueryBaseBuilder oe = sharedData.userDefinedName2OpMap.get(name);
    if (oe != null && oe != this) {
      throw new IllegalArgumentException("Operator with name :\"" + name + "\" already exists: " + oe.op);
    }

    String oldName = sharedData.op2UserDefinedNameMap.get(this);
    if (oldName != null) {
      sharedData.userDefinedName2OpMap.remove(name);
      sharedData.op2UserDefinedNameMap.remove(this);
    }
    sharedData.userDefinedName2OpMap.put(name, this);
    sharedData.op2UserDefinedNameMap.put(this, name);

    if (op instanceof AbstractConsumerEncoding) {
      sharedData.op2UserDefinedNameMap.put(children[0], name + "P");
    }

    return this;
  }

  @Override
  public String toString() {
    return "Operator: " + op;
  }

  @Override
  public JsonQueryBaseBuilder beginIterate(final StreamingStateEncoding<?> idbStateProcessor) {
    JsonQueryBaseBuilder ibJBB = buildOperator(IterateBeginPlaceHolder.class, "argChild", this, NO_PREFERENCE);
    ((IterateBeginPlaceHolder) ibJBB.op).idbStateProcessor = idbStateProcessor;
    return ibJBB;
  }

  @Override
  public JsonQueryBaseBuilder beginIterate() {
    return beginIterate(new DupElimStateEncoding());
  }

  @Override
  public JsonQueryBaseBuilder endIterate(final JsonQueryBuilder iterateBeginner) {
    Preconditions.checkArgument(((JsonQueryBaseBuilder) iterateBeginner).op instanceof IterateBeginPlaceHolder,
        "Iterate beginner must be a " + JsonQueryBuilder.class.getSimpleName() + " generated by iterateBegin");

    JsonQueryBaseBuilder ieJBB = buildOperator(IterateEndPlaceHolder.class, "argChild", this, NO_PREFERENCE);
    ((IterateEndPlaceHolder) ieJBB.op).iterationBeginPoint = (JsonQueryBaseBuilder) iterateBeginner;

    // initial input and iterate input must be compatible in workers.
    if (workerSetAlgebra(((JsonQueryBaseBuilder) iterateBeginner).runOnWorkers, ieJBB.runOnWorkers) == null) {
      throw new IllegalArgumentException("Workers of initial input must be compatible with workers of iterate input.");
    }

    return ieJBB;
  }

  /**
   * A helper class to mark the place where the iteration starts.
   * */
  private static final class IterateBeginPlaceHolder extends OperatorEncoding<IDBController> {

    /**
     * IDB state processor.
     * */
    private StreamingStateEncoding<?> idbStateProcessor;

    /**
     * Used by java reflection.
     * */
    @SuppressWarnings("unused")
    public IterateBeginPlaceHolder() {
    }

    @Override
    public void connect(final Operator operator, final Map<String, Operator> operators) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IDBController construct(final Server server) throws MyriaApiException {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * A helper class to mark the place where the iteration ends.
   * */
  private static final class IterateEndPlaceHolder extends OperatorEncoding<IDBController> {

    /**
     * The corresponding iteration begin placeholder.
     * */
    private JsonQueryBaseBuilder iterationBeginPoint;

    /**
     * Used by java reflection.
     * */
    @SuppressWarnings("unused")
    public IterateEndPlaceHolder() {
    }

    @Override
    public void connect(final Operator operator, final Map<String, Operator> operators) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IDBController construct(final Server server) throws MyriaApiException {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * {@link Aggregate}. Count.
   * 
   * @return builder.
   * */
  public JsonQueryBaseBuilder count() {
    return aggregate(new int[] { 0 }, new int[] { Aggregator.AGG_OP_COUNT });
  }

}
