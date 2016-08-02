package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.expression.evaluate.PythonUDFEvaluator;
import edu.washington.escience.myria.expression.evaluate.ScriptEvalInterface;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.storage.Tuple;

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

  /**
   * The states that are passed during execution.
   */
  private transient Tuple state;
  /**
   * Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updaters}.
   */
  private transient ScriptEvalInterface updateEvaluator;
  private transient ArrayList<PythonUDFEvaluator> pyUpdateEvaluators;
  /**
   * One evaluator for each expression in {@link #emitters}.
   */
  private transient ArrayList<GenericEvaluator> emitEvaluators;

  /**
   * The schema of the result tuples.
   */
  private transient Schema resultSchema;

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
    state = null;
    updateEvaluator = null;
    pyUpdateEvaluators = null;
    emitEvaluators = null;
    resultSchema = null;
  }

  @Override
  public Aggregator get(final Schema inputSchema) throws DbException {
    throw new DbException("should call get with pyFuncReg");
  }

  @Override
  @Nonnull
  public Aggregator get(final Schema inputSchema, final PythonFunctionRegistrar pyFuncReg)
      throws DbException {
    List<Integer> needsPyEval = Lists.newLinkedList();
    if (state == null) {
      Objects.requireNonNull(inputSchema, "inputSchema");
      Preconditions.checkArgument(
          initializers.size() == updaters.size(),
          "must have the same number of aggregate state initializers (%s) and updaters (%s)",
          initializers.size(),
          updaters.size());
      // Verify that initializers and updaters have compatible names
      for (int i = 0; i < initializers.size(); i++) {
        Preconditions.checkArgument(
            Objects.equals(initializers.get(i).getOutputName(), updaters.get(i).getOutputName()),
            "initializers[i] and updaters[i] have different names (%s) != (%s)",
            initializers.get(i).getOutputName(),
            updaters.get(i).getOutputName());
      }

      /* Initialize the state. */
      Schema stateSchema = generateStateSchema(inputSchema);
      state = new Tuple(stateSchema);
      ScriptEvalInterface stateEvaluator =
          getEvalScript(initializers, new ExpressionOperatorParameter(inputSchema), null, null);
      stateEvaluator.evaluate(null, 0, state, null);

      /* Set up the updaters. */

      pyUpdateEvaluators = new ArrayList<>();
      // emitEvaluators.ensureCapacity(emitters.size());

      updateEvaluator =
          getEvalScript(
              updaters,
              new ExpressionOperatorParameter(inputSchema, stateSchema),
              pyFuncReg,
              needsPyEval);

      /* Set up the emitters. */
      emitEvaluators = new ArrayList<>();
      emitEvaluators.ensureCapacity(emitters.size());

      for (Expression expr : emitters) {
        GenericEvaluator evaluator;
        if (expr.isRegisteredUDF()) {
          evaluator =
              new PythonUDFEvaluator(
                  expr, new ExpressionOperatorParameter(inputSchema, stateSchema), pyFuncReg);
        } else {
          evaluator =
              new GenericEvaluator(expr, new ExpressionOperatorParameter(null, stateSchema));
        }

        evaluator.compile();
        emitEvaluators.add(evaluator);
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
    }
    return new UserDefinedAggregator(
        state.clone(),
        updateEvaluator,
        pyUpdateEvaluators,
        emitEvaluators,
        resultSchema,
        needsPyEval);
  }

  /**
   * Produce a {@link ScriptEvalInterface} from {@link Expression}s and {@link ExpressionOperatorParameter}s. This
   * function produces the code for a Java script that executes all expressions in turn and appends the calculated
   * values to the result. The values to be output are calculated completely before they are stored to the output, thus
   * it is safe to pass the same object as input and output, e.g., in the case of updating state in an Aggregate.
   *
   * @param expressions one expression for each output column.
   * @param param the inputs that expressions may use, including the {@link Schema} of the expression inputs and
   *          worker-local variables.
   * @return a compiled object that will run all the expressions and store them into the output.
   * @throws DbException if there is an error compiling the expressions.
   */
  private ScriptEvalInterface getEvalScript(
      @Nonnull final List<Expression> expressions,
      @Nonnull final ExpressionOperatorParameter param,
      final PythonFunctionRegistrar pyFuncReg,
      final List<Integer> needsPyEval)
      throws DbException {

    StringBuilder compute = new StringBuilder();
    StringBuilder output = new StringBuilder();
    LOGGER.info("expression size" + expressions.size());

    for (int varCount = 0; varCount < expressions.size(); ++varCount) {
      Expression expr = expressions.get(varCount);
      if (!expr.isRegisteredUDF()) {
        Type type = expr.getOutputType(param);

        // type valI = expression;
        compute
            .append(type.toJavaType().getName())
            .append(" val")
            .append(varCount)
            .append(" = ")
            .append(expr.getJavaExpression(param))
            .append(";\n");

        // result.putType(I, valI);
        output
            .append(Expression.RESULT)
            .append(".put")
            .append(type.toJavaObjectType().getSimpleName())
            .append("(")
            .append(varCount)
            .append(", val")
            .append(varCount)
            .append(");\n");
      } else {
        PythonUDFEvaluator evaluator = new PythonUDFEvaluator(expr, param, pyFuncReg);
        pyUpdateEvaluators.add(evaluator);
        needsPyEval.add(varCount);
      }
    }
    if (needsPyEval != null) {
      LOGGER.info("number of python evaluators: " + needsPyEval.size());
    }

    String script = compute.append(output).toString();
    LOGGER.info("Compiling UDA {}", script);

    IScriptEvaluator se;
    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception e) {
      LOGGER.error("Could not create scriptevaluator", e);
      throw new DbException("Could not create scriptevaluator", e);
    }
    se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);

    try {
      return (ScriptEvalInterface)
          se.createFastEvaluator(
              script,
              ScriptEvalInterface.class,
              new String[] {Expression.TB, Expression.ROW, Expression.RESULT, Expression.STATE});
    } catch (CompileException e) {
      LOGGER.error("Error when compiling expression {}: {}", script, e);
      throw new DbException("Error when compiling expression: " + script, e);
    }
  }

  /**
   * Generate the schema of the state.
   *
   * @param inputSchema the {@link Schema} of the input tuples.
   * @return the {@link Schema} of the state assuming the specified input types.
   */
  private Schema generateStateSchema(final Schema inputSchema) {
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();

    for (Expression expr : initializers) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema)));
      namesBuilder.add(expr.getOutputName());
    }
    return new Schema(typesBuilder.build(), namesBuilder.build());
  }
}
