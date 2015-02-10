package edu.washington.escience.myria.api.encoding;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.ws.rs.core.Response.Status;

import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
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
import edu.washington.escience.myria.operator.SinkRoot;
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
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QueryEncoding.class);

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
    /* First, we need to know which workers run on each plan. */
    setupWorkersForFragments(fragments, args);
    /* Next, we need to know which pipes (operators) are produced and consumed on which workers. */
    setupWorkerNetworkOperators(fragments);

    HashMap<Integer, PlanFragmentEncoding> op2OwnerFragmentMapping = Maps.newHashMap();
    int idx = 0;
    for (PlanFragmentEncoding fragment : fragments) {
      fragment.setFragmentIndex(idx++);
      for (OperatorEncoding<?> op : fragment.operators) {
        op2OwnerFragmentMapping.put(op.opId, fragment);
      }
    }

    Map<Integer, SubQueryPlan> plan = Maps.newHashMap();
    HashMap<PlanFragmentEncoding, RootOperator> instantiatedFragments = Maps.newHashMap();
    HashMap<Integer, Operator> allOperators = Maps.newHashMap();
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
   * Figures out which workers are needed for every fragment.
   * 
   * @throws CatalogException if there is an error in the Catalog.
   */
  private static void setupWorkersForFragments(final List<PlanFragmentEncoding> fragments, final ConstructArgs args)
      throws CatalogException {
    Server server = args.getServer();
    for (PlanFragmentEncoding fragment : fragments) {
      if (fragment.overrideWorkers != null && fragment.overrideWorkers.size() > 0) {
        /* The workers are set in the plan. */
        fragment.workers = fragment.overrideWorkers;
        continue;
      }

      /* The workers are *not* set in the plan. Let's find out what they are. */
      fragment.workers = Lists.newArrayList();
      /* Set this flag if we encounter an operator that implies this fragment must run on at most one worker. */
      OperatorEncoding<?> singletonOp = null;

      /* If the plan has scans, it has to run on all of those workers. */
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
        } else if (operator instanceof CollectConsumerEncoding || operator instanceof SingletonEncoding) {
          singletonOp = operator;
          continue;
        } else {
          continue;
        }
        if (scanWorkers == null) {
          throw new MyriaApiException(Status.BAD_REQUEST, "Unable to find workers that store " + scanRelation);
        }
        if (fragment.workers.size() == 0) {
          fragment.workers.addAll(scanWorkers);
        } else {
          /*
           * If the fragment already has workers, it scans multiple relations. They better use the exact same set of
           * workers.
           */
          if (fragment.workers.size() != scanWorkers.size() || !fragment.workers.containsAll(scanWorkers)) {
            throw new MyriaApiException(Status.BAD_REQUEST,
                "All tables scanned within a fragment must use the exact same set of workers. Caught scanning "
                    + scanRelation);
          }
        }
      }
      if (fragment.workers.size() > 0) {
        if (singletonOp != null && fragment.workers.size() != 1) {
          throw new MyriaApiException(Status.BAD_REQUEST, "A fragment with " + singletonOp
              + " requires exactly one worker, but " + fragment.workers.size() + " workers specified.");
        }
        continue;
      }

      /* No workers pre-specified / no scans found. Is there a singleton op? */
      if (singletonOp != null) {
        /* Just pick the first alive worker. */
        fragment.workers.add(server.getAliveWorkers().iterator().next());
        continue;
      }

      /* If not, just add all the alive workers in the cluster. */
      fragment.workers.addAll(server.getAliveWorkers());
    }
  }

  /**
   * Loop through all the operators in a plan fragment and connect them up.
   */
  private static void setupWorkerNetworkOperators(final List<PlanFragmentEncoding> fragments) {
    SetMultimap<Integer, Integer> producerWorkerMap = HashMultimap.create();
    SetMultimap<ExchangePairID, Integer> consumerWorkerMap = HashMultimap.create();
    ListMultimap<Integer, ExchangePairID> producerOutputChannels = ArrayListMultimap.create();
    List<IDBControllerEncoding> idbControllers = Lists.newArrayList();

    /* Pass 1: map strings to real operator IDs, also collect producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof AbstractConsumerEncoding) {
          AbstractConsumerEncoding<?> consumer = (AbstractConsumerEncoding<?>) operator;
          Integer sourceProducerID = consumer.getArgOperatorId();
          ExchangePairID channelID = ExchangePairID.newID();
          consumer.setRealOperatorIds(ImmutableList.of(channelID));
          producerOutputChannels.put(sourceProducerID, channelID);
          consumerWorkerMap.putAll(channelID, fragment.workers);
        } else if (operator instanceof AbstractProducerEncoding) {
          AbstractProducerEncoding<?> producer = (AbstractProducerEncoding<?>) operator;
          producer.setRealOperatorIds(producerOutputChannels.get(producer.opId));
          producerWorkerMap.putAll(producer.opId, fragment.workers);
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbController = (IDBControllerEncoding) operator;
          idbControllers.add(idbController);
          producerWorkerMap.putAll(idbController.opId, fragment.workers);
        }
      }
    }
    for (IDBControllerEncoding idbController : idbControllers) {
      List<ExchangePairID> ids = producerOutputChannels.get(idbController.opId);
      Preconditions.checkNotNull(ids, "Can't find channel IDs for IDBController opId=%s", idbController.opId);
      Preconditions.checkArgument(ids.size() == 1, "IDBController opId=%s has zero or multiple output channels",
          idbController.opId);
      idbController.setRealEosControllerOperatorID(ids.get(0));
    }
    /* Pass 2: Populate the right fields in producers and consumers. */
    for (PlanFragmentEncoding fragment : fragments) {
      for (OperatorEncoding<?> operator : fragment.operators) {
        if (operator instanceof ExchangeEncoding) {
          ExchangeEncoding exchange = (ExchangeEncoding) operator;
          ImmutableSet.Builder<Integer> workers = ImmutableSet.builder();
          for (ExchangePairID id : exchange.getRealOperatorIds()) {
            if (exchange instanceof AbstractConsumerEncoding) {
              int argOpId = ((AbstractConsumerEncoding<?>) exchange).getArgOperatorId();
              Set<Integer> producerWorkers = producerWorkerMap.get(argOpId);
              /* Use checkArgument instead of checkNotNull to throw an IllegalArgumentException */
              Preconditions.checkArgument(producerWorkers != null,
                  "Can't find corresponding producer for consumer opId=%s, argOperatorId: %s", operator.opId, argOpId);
              workers.addAll(producerWorkers);
            } else if (exchange instanceof AbstractProducerEncoding) {
              Set<Integer> consumerWorkers = consumerWorkerMap.get(id);
              Preconditions.checkNotNull(consumerWorkers, "Can't find corresponding consumer for producer opId=%s",
                  operator.opId);
              workers.addAll(consumerWorkers);
            } else {
              throw new IllegalStateException("ExchangeEncoding " + operator.getClass().getSimpleName()
                  + " is not a Producer or Consumer encoding");
            }
          }
          exchange.setRealWorkerIds(workers.build());
        } else if (operator instanceof IDBControllerEncoding) {
          IDBControllerEncoding idbController = (IDBControllerEncoding) operator;
          ExchangePairID id = idbController.getRealEosControllerOperatorID();
          Preconditions.checkNotNull(id, "Can't get real EOSController operator ID for IDBController opId=%s",
              operator.opId);
          Set<Integer> consumerWorkers = consumerWorkerMap.get(id);
          Preconditions.checkNotNull(consumerWorkers, "Can't find corresponding consumers for IDBController opId=%s",
              operator.opId);
          Preconditions.checkArgument(consumerWorkers.size() == 1,
              "The consumer corresponds to IDBController opId=%s lives on more than one worker", operator.opId);
          idbController.realEosControllerWorkerId = consumerWorkers.iterator().next();
        }
      }
    }

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
      final HashMap<PlanFragmentEncoding, RootOperator> instantiatedFragments,
      final Map<Integer, PlanFragmentEncoding> opOwnerFragment, final Map<Integer, Operator> allOperators) {
    RootOperator instantiatedFragment = instantiatedFragments.get(planFragment);
    if (instantiatedFragment != null) {
      return instantiatedFragment;
    }

    RootOperator fragmentRoot = null;
    CollectConsumer oldRoot = null;
    Map<Integer, Operator> myOperators = Maps.newHashMap();
    HashMap<Integer, AbstractConsumerEncoding<?>> nonIterativeConsumers = Maps.newHashMap();
    HashSet<IDBControllerEncoding> idbs = Sets.newHashSet();
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
      if (op instanceof CollectConsumer) {
        oldRoot = (CollectConsumer) op;
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
      Operator initialInput = idbOp.getChildren()[IDBController.CHILDREN_IDX_INITIAL_IDB_INPUT];
      Consumer iterativeInput = (Consumer) idbOp.getChildren()[IDBController.CHILDREN_IDX_ITERATION_INPUT];
      Consumer eosControllerInput = (Consumer) idbOp.getChildren()[IDBController.CHILDREN_IDX_EOS_CONTROLLER_INPUT];
      iterativeInput.setSchema(initialInput.getSchema());
      eosControllerInput.setSchema(EOSController.EOS_REPORT_SCHEMA);
    }
    /* Return the root. */
    if (fragmentRoot == null && oldRoot != null) {
      /* Old query plan, add a SinkRoot to the top. */
      LOGGER.info("Adding a SinkRoot to the top of an old query plan.");
      fragmentRoot = new SinkRoot(oldRoot);
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
