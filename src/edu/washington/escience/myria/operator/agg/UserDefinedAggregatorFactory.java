package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.PythonUDFEvaluator;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;

/**
 * Apply operator that has to be initialized and carries a state while new tuples are generated.
 */
public class UserDefinedAggregatorFactory implements AggregatorFactory {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(UserDefinedAggregatorFactory.class);

  /** Expressions that initialize the state variables. */
  @JsonProperty private final List<Expression> initializers;
  /** Expressions that update the state variables as a function of the input and the current tuple. */
  @JsonProperty private final List<Expression> updaters;
  /** Expressions that emit the final aggregation result from the state. */
  @JsonProperty private final List<Expression> emitters;
  /** Evaluators that initialize the {@link #state}. */
  private List<GenericEvaluator> initEvaluators;
  /** Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updaters}. */
  private List<GenericEvaluator> updateEvaluators;
  /** The schema of the result tuples. */
  private Schema resultSchema;

  /**
   * Construct a new user-defined aggregate. The initializers set the initial state of the aggregate; the updaters
   * update this state for every new tuple. The emitters produce the final value of the aggregate. Note that there must
   * be the same number of initializers and updaters, but there may be any number > 0 of emitters.
   *
   * @param initializers Expressions that initialize the state variables.
   * @param updaters Expressions that update the state variables as a function of the input and the current tuple.
   * @param emitters Expressions that emit the final aggregation result from the state.
   */
  @JsonCreator
  public UserDefinedAggregatorFactory(
      @JsonProperty(value = "initializers", required = true) final List<Expression> initializers,
      @JsonProperty(value = "updaters", required = true) final List<Expression> updaters,
      @JsonProperty(value = "emitters", required = true) final List<Expression> emitters) {
    this.initializers = Objects.requireNonNull(initializers, "initializers");
    this.updaters = Objects.requireNonNull(updaters, "updaters");
    this.emitters = Objects.requireNonNull(emitters, "emitters");
    resultSchema = null;
    updateEvaluators = new ArrayList<GenericEvaluator>();
    initEvaluators = new ArrayList<GenericEvaluator>();
  }

  @Override
  public List<Expression> generateEmitExpressions(final Schema inputSchema) throws DbException {
    return emitters;
  }

  @Override
  public List<Aggregator> generateInternalAggs(final Schema inputSchema) throws DbException {
    /* Initialize the state. */
    for (int i = 0; i < initializers.size(); ++i) {
      initEvaluators.add(
          getEvaluator(initializers.get(i), new ExpressionOperatorParameter(inputSchema), i));
    }

    /* Set up the updaters. */
    Schema stateSchema = generateStateSchema(inputSchema);
    ExpressionOperatorParameter param =
        new ExpressionOperatorParameter(inputSchema, stateSchema, pyFuncReg);
    for (int i = 0; i < updaters.size(); ++i) {
      if (updaters.get(i).isRegisteredPythonUDF()) {
        updateEvaluators.add(new PythonUDFEvaluator(updaters.get(i), param));
      } else {
        updateEvaluators.add(getEvaluator(updaters.get(i), param, i));
      }
    }

    /* Compute the result schema. */
    ExpressionOperatorParameter emitParams = new ExpressionOperatorParameter(null, stateSchema);
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (Expression e : emitters) {
      types.add(e.getOutputType(emitParams));
      names.add(e.getOutputName());
    }
    resultSchema = new Schema(types, names);
    return ImmutableList.of(
        new UserDefinedAggregator(initEvaluators, updateEvaluators, resultSchema, stateSchema));
  }

  /**
   * Produce a {@link GenericEvaluator} from {@link Expression} and {@link ExpressionOperatorParameter}s. This function
   * produces the code for a Java script that executes all expressions in turn and appends the calculated values to the
   * result. The values to be output are calculated completely before they are stored to the output, thus it is safe to
   * pass the same object as input and output, e.g., in the case of updating state in an Aggregate.
   *
   * @param expressions one expression for each output column.
   * @param param the inputs that expressions may use, including the {@link Schema} of the expression inputs and
   *        worker-local variables.
   * @param col the column index of the expression.
   * @return a compiled object that will run all the expressions and store them into the output.
   * @throws DbException if there is an error compiling the expressions.
   */
  private GenericEvaluator getEvaluator(
      @Nonnull final Expression expr,
      @Nonnull final ExpressionOperatorParameter param,
      final int col)
      throws DbException {
    StringBuilder compute = new StringBuilder();

    Type type = expr.getOutputType(param);
    // <TYPE> val<I> = <EXPRESSION>;
    compute
        .append(type.toJavaType().getName())
        .append(" val")
        .append(col)
        .append(" = ")
        .append(expr.getJavaExpression(param))
        .append(";\n");

    if (param.getStateSchema() == null) {
      // state.put<TYPE>(<I>, val<I>);
      compute
          .append(Expression.STATE)
          .append(".put")
          .append(type == Type.BLOB_TYPE ? "Blob" : type.toJavaObjectType().getSimpleName())
          .append("(")
          .append(col)
          .append("+")
          .append(Expression.STATECOLOFFSET)
          .append(", val")
          .append(col)
          .append(");\n");
    } else {
      // state.replace<TYPE>(<I> + stateColOffset, stateRow, val<I>);
      compute
          .append(Expression.STATE)
          .append(".replace")
          .append(type == Type.BLOB_TYPE ? "Blob" : type.toJavaObjectType().getSimpleName())
          .append("(")
          .append(col)
          .append("+")
          .append(Expression.STATECOLOFFSET)
          .append(", ")
          .append(Expression.STATEROW)
          .append(", val")
          .append(col)
          .append(");\n");
    }

    String script = compute.toString();
    LOGGER.debug("Compiling UDA {}", script);

    IScriptEvaluator se;
    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception e) {
      LOGGER.debug("Could not create scriptevaluator", e);
      throw new DbException("Could not create scriptevaluator", e);
    }
    se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);
    GenericEvaluator eval = new GenericEvaluator(expr, script, param);
    return eval;
  }

  @Override
  public Schema generateSchema(final Schema inputSchema) {
    Schema stateSchema = generateStateSchema(inputSchema);
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
    for (Expression expr : emitters) {
      typesBuilder.add(
          expr.getOutputType(new ExpressionOperatorParameter(inputSchema, stateSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return Schema.of(typesBuilder.build(), namesBuilder.build());
  }

  @Override
  public Schema generateStateSchema(final Schema inputSchema) {
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
    for (Expression expr : initializers) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return Schema.of(typesBuilder.build(), namesBuilder.build());
  }

  PythonFunctionRegistrar pyFuncReg;

  public void setPyFuncReg(PythonFunctionRegistrar pyFuncReg) {
    this.pyFuncReg = pyFuncReg;
  }
}
