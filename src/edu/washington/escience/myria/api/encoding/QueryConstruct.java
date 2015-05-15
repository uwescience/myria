package edu.washington.escience.myria.api.encoding;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.IDBController;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.UpdateCatalog;
import edu.washington.escience.myria.operator.agg.MultiGroupByAggregate;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.EOSController;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.JsonSubQuery;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SubQuery;
import edu.washington.escience.myria.parallel.SubQueryPlan;

public class QueryConstruct {

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(QueryConstruct.class);

  /**
   * Instantiate the server's desired physical plan from a list of JSON encodings of fragments. This list must contain a
   * self-consistent, complete query. All fragments will be executed in parallel.
   * 
   * @param fragments the JSON-encoded query fragments to be executed in parallel
   * @param server the server on which the query will be executed
   * @return the physical plan
   * @throws CatalogException if there is an error instantiating the plan
   */
  public static Map<Integer, SubQueryPlan> instantiate(final List<PlanFragmentEncoding> fragments,
      final ConstructArgs args) throws CatalogException {

    // Assign fragment index before everything else
    int idx = 0;
    for (PlanFragmentEncoding fragment : fragments) {
      fragment.setFragmentIndex(idx++);
    }

    /* Sanity check the edges between fragments. */
    sanityCheckEdges(fragments);

    assignWorkersToFragments(fragments, args);

    Map<Integer, PlanFragmentEncoding> op2OwnerFragmentMapping = Maps.newHashMap();
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> op : fragment.operators) {
        op2OwnerFragmentMapping.put(op.opId, fragment);
      }
    }

    Map<Integer, SubQueryPlan> plan = Maps.newHashMap();
    Map<PlanFragmentEncoding, RootOperator> instantiatedFragments = Maps.newHashMap();
    Map<Integer, Operator> allOperators = Maps.newHashMap();
    for (PlanFragmentEncoding fragment : fragments) {
      RootOperator op =
          instantiateFragment(fragment, args, instantiatedFragments, op2OwnerFragmentMapping, allOperators);
      for (Integer worker : fragment.workers) {
        SubQueryPlan workerPlan = plan.get(worker);
        if (workerPlan == null) {
          workerPlan = new SubQueryPlan();
          plan.put(worker, workerPlan);
        }
        workerPlan.addRootOp(op);
      }
    }
    return plan;
  }

  /**
   * Set the query execution options for the specified plans.
   * 
   * @param plans the physical query plan
   * @param ftMode the fault tolerance mode under which the query will be executed
   * @param profilingMode how the query should be profiled
   */
  public static void setQueryExecutionOptions(final Map<Integer, SubQueryPlan> plans, final FTMode ftMode,
      @Nonnull final Set<ProfilingMode> profilingMode) {
    for (SubQueryPlan plan : plans.values()) {
      plan.setFTMode(ftMode);
      plan.setProfilingMode(profilingMode);
    }
  }

  /**
   * Helper function for setting the workers of a fragment. If the workers are not yet set, always succeeds. If the
   * workers are set, ensures that the new value exactly matches the old value.
   * 
   * @param fragment the fragment
   * @param workers the workers this fragment should be assigned to
   * @return <code>true</code> if the workers were newly assigned
   * @throws IllegalArgumentException if the fragment already has workers, and the new set does not match
   */
  private static boolean setOrVerifyFragmentWorkers(@Nonnull final PlanFragmentEncoding fragment,
      @Nonnull final Collection<Integer> workers, @Nonnull final String currentTask) {
    Preconditions.checkNotNull(fragment, "fragment");
    Preconditions.checkNotNull(workers, "workers");
    Preconditions.checkNotNull(currentTask, "currentTask");
    if (fragment.workers == null) {
      fragment.workers = ImmutableList.copyOf(workers);
      return true;
    } else {
      Preconditions.checkArgument(HashMultiset.create(fragment.workers).equals(HashMultiset.create(workers)),
          "During %s, cannot change workers for fragment %s from %s to %s", currentTask, fragment.fragmentIndex,
          fragment.workers, workers);
      return false;
    }
  }

  /**
   * Use the Catalog to set the workers for fragments that have scans, and verify that the workers are consistent with
   * existing constraints.
   * 
   * @see #assignWorkersToFragments(List, ConstructArgs)
   * 
   * @param fragments the fragments of the plan
   * @param args other arguments necessary for query construction
   * @throws CatalogException if there is an error getting information from the Catalog
   */
  private static void setAndVerifyScans(final List<PlanFragmentEncoding> fragments, final ConstructArgs args)
      throws CatalogException {
    Server server = args.getServer();

    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        Set<Integer> scanWorkers;
        String scanRelation;

        if (operator instanceof TableScanEncoding) {
          TableScanEncoding scan = ((TableScanEncoding) operator);
          scanRelation = scan.relationKey.toString();
          scanWorkers = server.getWorkersForRelation(scan.relationKey, scan.storedRelationId);
        } else if (operator instanceof TempTableScanEncoding) {
          TempTableScanEncoding scan = ((TempTableScanEncoding) operator);
          scanRelation = "temporary relation " + scan.table;
          scanWorkers =
              server.getQueryManager().getWorkersForTempRelation(args.getQueryId(),
                  RelationKey.ofTemp(args.getQueryId(), scan.table));
        } else {
          continue;
        }
        Preconditions.checkArgument(scanWorkers != null, "Unable to find workers that store %s", scanRelation);
        /*
         * Note: the current assumption is that all the partitions need to be scanned. This will not be true if we have
         * data replication, or allow to scan only a subset of the partitions. Revise if needed.
         */
        setOrVerifyFragmentWorkers(fragment, scanWorkers, "Setting workers for " + scanRelation);
      }
    }
  }

  /**
   * Verify that a plan meets the basic sanity checks. E.g., every producer should have a consumer. Only producers that
   * support multiple consumers (LocalMultiwayProducer, EOSController) can have multiple consumers.
   * 
   * @see #assignWorkersToFragments(List, ConstructArgs)
   * 
   * @param fragments the fragments of the plan
   */
  public static void sanityCheckEdges(final List<PlanFragmentEncoding> fragments) {
    /* These maps connect each channel id to the fragment that produces or consumes it. */
    // producers must be unique
    Map<Integer, PlanFragmentEncoding> producerMap = Maps.newHashMap();
    // consumers can be repeated, as long as the producer is a LocalMultiwayProducer
    Multimap<Integer, PlanFragmentEncoding> consumerMap = ArrayListMultimap.create();
    final Set<Integer> soleConsumer = Sets.newHashSet();

    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        /* Build the producer/consumer map. */
        if (operator instanceof AbstractConsumerEncoding) {
          AbstractConsumerEncoding<?> consumer = (AbstractConsumerEncoding<?>) operator;
          consumerMap.put(consumer.argOperatorId, fragment);
        } else if (operator instanceof AbstractProducerEncoding || operator instanceof IDBControllerEncoding) {
          Integer opId = operator.opId;
          PlanFragmentEncoding oldFragment = producerMap.put(opId, fragment);
          if (oldFragment != null) {
            Preconditions.checkArgument(false,
                "Two different operators cannot produce the same opId %s. Fragments: %s %s", opId,
                fragment.fragmentIndex, oldFragment.fragmentIndex);
          }
          if (!(operator instanceof LocalMultiwayProducerEncoding || operator instanceof EOSControllerEncoding)) {
            soleConsumer.add(opId);
          }
        }
      }
    }

    /* Sanity check 1: Producer must have corresponding consumers, and vice versa. */
    Set<Integer> consumedNotProduced = Sets.difference(consumerMap.keySet(), producerMap.keySet());
    Preconditions.checkArgument(consumedNotProduced.isEmpty(), "Missing producer(s) for consumer(s): %s",
        consumedNotProduced);
    Set<Integer> producedNotConsumed = Sets.difference(producerMap.keySet(), consumerMap.keySet());
    Preconditions.checkArgument(producedNotConsumed.isEmpty(), "Missing consumer(s) for producer(s): %s",
        producedNotConsumed);

    /* Sanity check 2: Operators that only admit a single consumer should have exactly one consumer. */
    for (Integer opId : soleConsumer) {
      Collection<PlanFragmentEncoding> consumers = consumerMap.get(opId);
      Preconditions.checkArgument(consumers.size() == 1, "Producer %s only supports a single consumer, not %s", opId,
          consumers.size());
    }
  }

  /**
   * Verify and propagate worker assignments of LocalMultiwayProducer/Consumer. Fragments containing
   * LocalMultiwayProducers/Consumers with the same operator ID need to be assigned to the same set of workers.
   * 
   * @see #assignWorkersToFragments(List, ConstructArgs)
   * 
   * @param fragments the fragments of the plan
   */
  private static void verifyAndPropagateLocalEdgeConstraints(final List<PlanFragmentEncoding> fragments) {
    // producers must be unique
    Map<Integer, PlanFragmentEncoding> producerMap = Maps.newHashMap();
    // consumers can be repeated, as long as the producer is a LocalMultiwayProducer
    Multimap<Integer, PlanFragmentEncoding> consumerMap = HashMultimap.create();

    /* Find the edges (identified by their opId) with equality constraints. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof LocalMultiwayConsumerEncoding) {
          LocalMultiwayConsumerEncoding consumer = (LocalMultiwayConsumerEncoding) operator;
          consumerMap.put(consumer.argOperatorId, fragment);
        } else if (operator instanceof LocalMultiwayProducerEncoding) {
          LocalMultiwayProducerEncoding producer = (LocalMultiwayProducerEncoding) operator;
          producerMap.put(producer.opId, fragment);
        }
      }
    }

    /* Verify and/or propagate these constraints. */
    Set<Integer> consumedNotProduced = Sets.difference(consumerMap.keySet(), producerMap.keySet());
    Preconditions.checkArgument(consumedNotProduced.isEmpty(), "Missing LocalMultiwayProducer(s) for consumer(s): %s",
        consumedNotProduced);
    Set<Integer> producedNotConsumed = Sets.difference(producerMap.keySet(), consumerMap.keySet());
    Preconditions.checkArgument(producedNotConsumed.isEmpty(), "Missing LocalMultiwayConsumer(s) for producer(s): %s",
        producedNotConsumed);

    boolean anyUpdates;
    do {
      anyUpdates = false;
      /* For each operator, verify that all producers and consumers have the same set of workers. */
      for (Integer opId : producerMap.keySet()) {
        List<PlanFragmentEncoding> allFrags = Lists.newLinkedList(consumerMap.get(opId));
        allFrags.add(producerMap.get(opId));

        // Find the set of workers assigned to any of them
        List<Integer> workers = null;
        for (PlanFragmentEncoding frag : allFrags) {
          if (frag.workers != null) {
            workers = frag.workers;
            break;
          }
        }

        // None -- skip this opId for now
        if (workers == null) {
          continue;
        }

        // Verify that all fragments match the workers we found (and propagate if null)
        for (PlanFragmentEncoding frag : allFrags) {
          anyUpdates |= setOrVerifyFragmentWorkers(frag, workers, "propagating edge constraints");
        }
      }
    } while (anyUpdates);
  }

  /**
   * Use the Catalog to set the workers for fragments that have scans, and verify that the workers are consistent with
   * existing constraints.
   * 
   * @see #assignWorkersToFragments(List, ConstructArgs)
   * 
   * @param fragments the fragments of the plan
   * @param args other arguments necessary for query construction
   * @throws CatalogException if there is an error getting information from the Catalog
   */
  private static void setAndVerifySingletonConstraints(final List<PlanFragmentEncoding> fragments,
      final ConstructArgs args) {
    List<Integer> singletonWorkers = ImmutableList.of(args.getServer().getAliveWorkers().iterator().next());

    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof CollectConsumerEncoding || operator instanceof SingletonEncoding
            || operator instanceof EOSControllerEncoding || operator instanceof BinaryFileScanEncoding
            || operator instanceof FileScanEncoding || operator instanceof NChiladaFileScanEncoding
            || operator instanceof SeaFlowFileScanEncoding || operator instanceof TipsyFileScanEncoding) {
          if (fragment.workers == null) {
            String encodingTypeName = operator.getClass().getSimpleName();
            String operatorTypeName = encodingTypeName.substring(0, encodingTypeName.indexOf("Encoding"));
            LOGGER.warn("{} operator can only be instantiated on a single worker, assigning to random worker",
                operatorTypeName);
            fragment.workers = singletonWorkers;
          } else {
            Preconditions.checkArgument(fragment.workers.size() == 1,
                "Fragment %s has a singleton operator %s, but workers %s", fragment.fragmentIndex, operator.opId,
                fragment.workers);
          }
          /* We only need to verify singleton-ness once per fragment. */
          break;
        }
      }
    }
  }

  /**
   * Actually allocate the real operator IDs and real worker IDs for the producers and consumers.
   * 
   * @see #assignWorkersToFragments(List, ConstructArgs)
   * 
   * @param fragments the fragments of the plan
   */
  private static void fillInRealOperatorAndWorkerIDs(final List<PlanFragmentEncoding> fragments) {
    Multimap<Integer, ExchangePairID> consumerMap = ArrayListMultimap.create();
    Map<Integer, List<Integer>> producerWorkerMap = Maps.newHashMap();
    Map<Integer, List<Integer>> consumerWorkerMap = Maps.newHashMap();

    /*
     * First pass: create a new ExchangePairID for each Consumer, and set it. Also track the workers for each producer
     * and consumer.
     */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof AbstractConsumerEncoding<?>) {
          AbstractConsumerEncoding<?> consumer = (AbstractConsumerEncoding<?>) operator;
          ExchangePairID exchangeId = ExchangePairID.newID();
          consumerMap.put(consumer.argOperatorId, exchangeId);
          consumerWorkerMap.put(consumer.argOperatorId, fragment.workers);
          consumer.setRealOperatorIds(ImmutableList.of(exchangeId));
        } else if (operator instanceof AbstractProducerEncoding<?> || operator instanceof IDBControllerEncoding) {
          producerWorkerMap.put(operator.opId, fragment.workers);
        }
      }
    }

    /* Second pass: set the ExchangePairIDs for each producer, also the workers for these and the consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof AbstractConsumerEncoding<?>) {
          AbstractConsumerEncoding<?> consumer = (AbstractConsumerEncoding<?>) operator;
          consumer.setRealWorkerIds(ImmutableSet.copyOf(producerWorkerMap.get(consumer.argOperatorId)));
        } else if (operator instanceof AbstractProducerEncoding<?>) {
          AbstractProducerEncoding<?> producer = (AbstractProducerEncoding<?>) operator;
          producer.setRealWorkerIds(ImmutableSet.copyOf(consumerWorkerMap.get(producer.opId)));
          producer.setRealOperatorIds(ImmutableList.copyOf(consumerMap.get(producer.opId)));
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbController = (IDBControllerEncoding) operator;
          idbController.realEosControllerWorkerId = consumerWorkerMap.get(idbController.opId).get(0);
          idbController.setRealEosControllerOperatorID(consumerMap.get(idbController.opId).iterator().next());
        }
      }
    }
  }

  /**
   * Given an abstract execution plan, assign the workers to the fragments.
   * 
   * This assignment follows the following five rules, in precedence order:
   * <ol>
   * <li>Obey user-overrides of fragment workers.</li>
   * <li>Fragments that scan tables must use the workers that contain the data.</li>
   * <li>Edge constraints between fragments. E.g., a {@link LocalMultiwayProducerEncoding} must use the same set of
   * workers as its consumer.</li>
   * <li>Singleton constraints: Fragments with a {@link CollectConsumerEncoding} or a {@link SingletonEncoding} must run
   * on a single worker. If none is still set, choose an arbitrary worker.</li>
   * <li>Unspecified: Any fragments that still have unspecified worker sets will use all workers in the cluster.</li>
   * </ol>
   * 
   * @param fragments
   * @param args
   * @throws CatalogException if there is an error getting information about existing relations from the catalog
   */
  private static void assignWorkersToFragments(final List<PlanFragmentEncoding> fragments, final ConstructArgs args)
      throws CatalogException {

    /* 1. Honor user overrides. Note this is unchecked, but we may find constraint violations later. */
    for (PlanFragmentEncoding fragment : fragments) {
      if (fragment.overrideWorkers != null && fragment.overrideWorkers.size() > 0) {
        /* The workers are set in the plan. */
        fragment.workers = fragment.overrideWorkers;
      }
    }

    /* 2. Use scans to set workers, and verify constraints. */
    setAndVerifyScans(fragments, args);

    /* 3. Verify and propagate worker assignments using LocalMultiwayProducer/Consumer constraints. */
    verifyAndPropagateLocalEdgeConstraints(fragments);

    /* 4. Use singletons to set worker, and verify constraints. */
    setAndVerifySingletonConstraints(fragments, args);

    /* 5. Again, verify and propagate worker assignments using LocalMultiwayProducer/Consumer constraints. */
    verifyAndPropagateLocalEdgeConstraints(fragments);

    /* Last-1. For all remaining fragments, fill them in with all workers. */
    Server server = args.getServer();
    ImmutableList<Integer> allWorkers = ImmutableList.copyOf(server.getAliveWorkers());
    for (PlanFragmentEncoding fragment : fragments) {
      if (fragment.workers == null) {
        fragment.workers = allWorkers;
      }
    }
    // We don't need to verify and propagate LocalMultiwayProducer/Consumer constraints again since all the new ones
    // have all workers.

    /* Fill in the #realOperatorIDs and the #realWorkerIDs fields for the producers and consumers. */
    fillInRealOperatorAndWorkerIDs(fragments);
  }

  /**
   * Given an encoding of a plan fragment, i.e., a connected list of operators, instantiate the actual plan fragment.
   * This includes instantiating the operators and connecting them together. The constraint on the plan fragments is
   * that the last operator in the fragment must be the RootOperator. There is a special exception for older plans in
   * which a CollectConsumer will automatically have a SinkRoot appended to it.
   * 
   * @param planFragment the encoded plan fragment.
   * @return the actual plan fragment.
   */
  private static RootOperator instantiateFragment(final PlanFragmentEncoding planFragment, final ConstructArgs args,
      final Map<PlanFragmentEncoding, RootOperator> instantiatedFragments,
      final Map<Integer, PlanFragmentEncoding> opOwnerFragment, final Map<Integer, Operator> allOperators) {
    RootOperator instantiatedFragment = instantiatedFragments.get(planFragment);
    if (instantiatedFragment != null) {
      return instantiatedFragment;
    }

    RootOperator fragmentRoot = null;
    Map<Integer, Operator> myOperators = Maps.newHashMap();
    Map<Integer, AbstractConsumerEncoding<?>> nonIterativeConsumers = Maps.newHashMap();
    Set<IDBControllerEncoding> idbs = Sets.newHashSet();
    /* Instantiate all the operators. */
    for (OperatorEncoding<?> encoding : planFragment.operators) {
      if (encoding instanceof IDBControllerEncoding) {
        idbs.add((IDBControllerEncoding) encoding);
      }
      if (encoding instanceof AbstractConsumerEncoding<?>) {
        nonIterativeConsumers.put(encoding.opId, (AbstractConsumerEncoding<?>) encoding);
      }

      Operator op = encoding.construct(args);
      /* helpful for debugging. */
      op.setOpName(MoreObjects.firstNonNull(encoding.opName, "Operator" + String.valueOf(encoding.opId)));
      op.setOpId(encoding.opId);
      op.setFragmentId(planFragment.fragmentIndex);
      myOperators.put(encoding.opId, op);
      if (op instanceof RootOperator) {
        if (fragmentRoot != null) {
          throw new MyriaApiException(Status.BAD_REQUEST, "Multiple " + RootOperator.class.getSimpleName()
              + " detected in the fragment: " + fragmentRoot.getOpName() + ", and " + encoding.opId);
        }
        fragmentRoot = (RootOperator) op;
      }
    }
    allOperators.putAll(myOperators);

    for (IDBControllerEncoding idb : idbs) {
      nonIterativeConsumers.remove(idb.argIterationInput);
      nonIterativeConsumers.remove(idb.argEosControllerInput);
    }

    Set<PlanFragmentEncoding> dependantFragments = Sets.newHashSet();
    for (AbstractConsumerEncoding<?> c : nonIterativeConsumers.values()) {
      dependantFragments.add(opOwnerFragment.get(c.argOperatorId));
    }

    for (PlanFragmentEncoding f : dependantFragments) {
      instantiateFragment(f, args, instantiatedFragments, opOwnerFragment, allOperators);
    }

    for (AbstractConsumerEncoding<?> c : nonIterativeConsumers.values()) {
      Consumer consumer = (Consumer) myOperators.get(c.opId);
      Integer producingOpName = c.argOperatorId;
      Operator producingOp = allOperators.get(producingOpName);
      if (producingOp instanceof IDBController) {
        consumer.setSchema(IDBController.EOI_REPORT_SCHEMA);
      } else {
        consumer.setSchema(producingOp.getSchema());
      }
    }

    /* Connect all the operators. */
    for (OperatorEncoding<?> encoding : planFragment.operators) {
      encoding.connect(myOperators.get(encoding.opId), myOperators);
    }

    for (IDBControllerEncoding idb : idbs) {
      IDBController idbOp = (IDBController) myOperators.get(idb.opId);
      Consumer eosControllerInput = (Consumer) idbOp.getChildren()[IDBController.CHILDREN_IDX_EOS_CONTROLLER_INPUT];
      eosControllerInput.setSchema(EOSController.EOS_REPORT_SCHEMA);
      Operator iterativeInput = idbOp.getChildren()[IDBController.CHILDREN_IDX_ITERATION_INPUT];
      if (iterativeInput instanceof Consumer) {
        Operator initialInput = idbOp.getChildren()[IDBController.CHILDREN_IDX_INITIAL_IDB_INPUT];
        ((Consumer) iterativeInput).setSchema(initialInput.getSchema());
        // Note: better to check if the producer of this iterativeInput also has the same schema, but can't find a good
        // place to add the check given the current framework.
      }
    }

    if (fragmentRoot == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "No " + RootOperator.class.getSimpleName()
          + " detected in the fragment.");
    }

    instantiatedFragments.put(planFragment, fragmentRoot);
    return fragmentRoot;
  }

  /**
   * Builds the query plan to update the {@link Server}'s master catalog with the number of tuples in every relation
   * written by a subquery. The query plan is basically "SELECT RelationKey, COUNT(*)" -> Collect at master ->
   * "SELECT RelationKey, SUM(counts)".
   * 
   * @param relationsWritten the metadata about which relations were written during the execution of this subquery.
   * @param server the server on which the catalog will be updated
   * @return the query plan to update the master's catalog with the new number of tuples for all written relations.
   */
  public static SubQuery getRelationTupleUpdateSubQuery(final Map<RelationKey, RelationWriteMetadata> relationsWritten,
      final Server server) {
    ExchangePairID collectId = ExchangePairID.newID();
    Schema schema =
        Schema.ofFields("userName", Type.STRING_TYPE, "programName", Type.STRING_TYPE, "relationName",
            Type.STRING_TYPE, "tupleCount", Type.LONG_TYPE);

    String dbms = server.getDBMS();
    Preconditions.checkState(dbms != null, "Server must have a configured DBMS environment variable");

    /*
     * Worker plans: for each relation, create a {@link DbQueryScan} to get the count, an {@link Apply} to add the
     * {@link RelationKey}, then a {@link CollectProducer} to send the count to the master.
     */
    Map<Integer, SubQueryPlan> workerPlans = Maps.newHashMap();
    for (RelationWriteMetadata meta : relationsWritten.values()) {
      Set<Integer> workers = meta.getWorkers();
      RelationKey relation = meta.getRelationKey();
      for (Integer worker : workers) {
        DbQueryScan localCount =
            new DbQueryScan("SELECT COUNT(*) FROM " + relation.toString(dbms), Schema.ofFields("tupleCount",
                Type.LONG_TYPE));
        List<Expression> expressions =
            ImmutableList.of(new Expression(schema.getColumnName(0), new ConstantExpression(relation.getUserName())),
                new Expression(schema.getColumnName(1), new ConstantExpression(relation.getProgramName())),
                new Expression(schema.getColumnName(2), new ConstantExpression(relation.getRelationName())),
                new Expression(schema.getColumnName(3), new VariableExpression(0)));
        Apply addRelationName = new Apply(localCount, expressions);
        CollectProducer producer = new CollectProducer(addRelationName, collectId, MyriaConstants.MASTER_ID);
        if (!workerPlans.containsKey(worker)) {
          workerPlans.put(worker, new SubQueryPlan(producer));
        } else {
          workerPlans.get(worker).addRootOp(producer);
        }
      }
    }

    /* Master plan: collect, sum, insert the updates. */
    CollectConsumer consumer = new CollectConsumer(schema, collectId, workerPlans.keySet());
    MultiGroupByAggregate aggCounts =
        new MultiGroupByAggregate(consumer, new int[] { 0, 1, 2 }, new SingleColumnAggregatorFactory(3,
            AggregationOp.SUM));
    UpdateCatalog catalog = new UpdateCatalog(aggCounts, server);
    SubQueryPlan masterPlan = new SubQueryPlan(catalog);

    return new SubQuery(masterPlan, workerPlans);
  }

  public static JsonSubQuery setDoWhileCondition(final String condition) {
    ImmutableList.Builder<PlanFragmentEncoding> fragments = ImmutableList.builder();
    int opId = 0;

    /* The worker part: scan the relation and send it to master. */
    // scan the relation
    TempTableScanEncoding scan = new TempTableScanEncoding();
    scan.opId = opId++;
    scan.opName = "Scan[" + condition + "]";
    scan.table = condition;
    // send it to master
    CollectProducerEncoding producer = new CollectProducerEncoding();
    producer.argChild = scan.opId;
    producer.opName = "CollectProducer[" + scan.opName + "]";
    producer.opId = opId++;
    // make a fragment
    PlanFragmentEncoding workerFragment = new PlanFragmentEncoding();
    workerFragment.operators = ImmutableList.of(scan, producer);
    // add it to the list
    fragments.add(workerFragment);

    /* The master part: collect the tuples, update the variable. */
    // collect the tuples
    CollectConsumerEncoding consumer = new CollectConsumerEncoding();
    consumer.argOperatorId = producer.opId;
    consumer.opId = opId++;
    consumer.opName = "CollectConsumer";
    // update the variable
    SetGlobalEncoding setGlobal = new SetGlobalEncoding();
    setGlobal.opId = opId++;
    setGlobal.opName = "SetGlobal[" + condition + "]";
    setGlobal.argChild = consumer.opId;
    setGlobal.key = condition;
    // the fragment, and it must only run at the master.
    PlanFragmentEncoding masterFragment = new PlanFragmentEncoding();
    masterFragment.operators = ImmutableList.of(consumer, setGlobal);
    masterFragment.overrideWorkers = ImmutableList.of(MyriaConstants.MASTER_ID);
    fragments.add(masterFragment);

    // Done!
    return new JsonSubQuery(fragments.build());
  }

  public final static class ConstructArgs {
    private final Server server;
    private final long queryId;

    public ConstructArgs(@Nonnull final Server server, final long queryId) {
      this.server = Preconditions.checkNotNull(server, "server");
      this.queryId = queryId;
    }

    public long getQueryId() {
      return queryId;
    }

    public Server getServer() {
      return server;
    }
  }
}
