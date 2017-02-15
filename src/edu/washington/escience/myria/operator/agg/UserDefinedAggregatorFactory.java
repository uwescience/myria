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
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(UserDefinedAggregatorFactory.class);

  /** Expressions that initialize the state variables. */
  @JsonProperty
  private final List<Expression> initializers;
  /** Expressions that update the state variables as a function of the input and the current tuple. */
  @JsonProperty
  private final List<Expression> updaters;
  /** Expressions that emit the final aggregation result from the state. */
  @JsonProperty
  private final List<Expression> emitters;

  /** Evaluators that initialize the {@link #state}. */
  private transient ScriptEvalInterface initEvaluator;
  /** Evaluators that update the {@link #state}. One evaluator for each expression in {@link #updaters}. */
  private transient ScriptEvalInterface updateEvaluator;
  /** One evaluator for each expression in {@link #emitters}. */
  private transient ArrayList<GenericEvaluator> emitEvaluators;
  /**
   * Evaluators for python expressions. One evaluator for each expression in {@link #updaters}.
   */
  private transient ArrayList<PythonUDFEvaluator> pyUpdateEvaluators;
  /**
   * Does the UDA have a pythonExpression?
   */
  private transient boolean bHasPyEval = false;

  /** The schema of the result tuples. */
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
    updateEvaluator = null;
    pyUpdateEvaluators = null;
    emitEvaluators = null;
    resultSchema = null;
  }

  @Override
  public List<Expression> generateEmitExpressions(final Schema inputSchema) throws DbException {
    return emitters;
  }

  @Override
  public List<Aggregator> generateInternalAggs(final Schema inputSchema) throws DbException {

    /* Initialize the state. */
    Schema stateSchema = generateStateSchema(inputSchema);
    initEvaluator = getEvalScript(initializers, new ExpressionOperatorParameter(inputSchema));

    /* Set up the updaters. */
    updateEvaluator = getEvalScript(updaters, new ExpressionOperatorParameter(inputSchema, stateSchema));

    /* Compute the result schema. */
    ExpressionOperatorParameter emitParams = new ExpressionOperatorParameter(null, stateSchema);
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (Expression e : emitters) {
      types.add(e.getOutputType(emitParams));
      names.add(e.getOutputName());
    }
    resultSchema = new Schema(types, names);
    return ImmutableList.of(new UserDefinedAggregator(initEvaluator, updateEvaluator, resultSchema, stateSchema));
  }

  /**
   * Produce a {@link ScriptEvalInterface} from {@link Expression}s and {@link ExpressionOperatorParameter}s. This
   * function produces the code for a Java script that executes all expressions in turn and appends the calculated
   * values to the result. The values to be output are calculated completely before they are stored to the output, thus
   * it is safe to pass the same object as input and output, e.g., in the case of updating state in an Aggregate.
   *
   * @param expressions one expression for each output column.
   * @param param the inputs that expressions may use, including the {@link Schema} of the expression inputs and
   *        worker-local variables.
   * @param pyFuncReg python function registrar.
   * @return a compiled object that will run all the expressions and store them into the output.
   * @throws DbException if there is an error compiling the expressions.
   */
  private ScriptEvalInterface getEvalScript(@Nonnull final List<Expression> expressions,
      @Nonnull final ExpressionOperatorParameter param) throws DbException {

    StringBuilder compute = new StringBuilder();
    StringBuilder output = new StringBuilder();

    for (int varCount = 0; varCount < expressions.size(); ++varCount) {
      Expression expr = expressions.get(varCount);
      if (expr.isRegisteredPythonUDF()) {
        throw new DbException("Python UDF expression can only be used as emit expressions");
      }
      Type type = expr.getOutputType(param);

      // type valI = expression;
      compute.append(type.toJavaType().getName()).append(" val").append(varCount).append(" = ")
          .append(expr.getJavaExpression(param)).append(";\n");

      if (param.getStateSchema() == null) {
        // state.putType(I, valI);
        output.append(Expression.STATE).append(".put").append(type.toJavaObjectType().getSimpleName()).append("(")
            .append(varCount).append("+").append(Expression.STATECOLOFFSET).append(", val").append(varCount)
            .append(");\n");
      } else {
        // state.replaceType(I, stateRow, valI);
        output.append(Expression.STATE).append(".replace").append(type.toJavaObjectType().getSimpleName()).append("(")
            .append(varCount).append("+").append(Expression.STATECOLOFFSET).append(", ").append(Expression.STATEROW)
            .append(", val").append(varCount).append(");\n");
      }
    }

    String script = compute.append(output).toString();
    LOGGER.debug("Compiling UDA {}", script);

    IScriptEvaluator se;
    try {
      se = CompilerFactoryFactory.getDefaultCompilerFactory().newScriptEvaluator();
    } catch (Exception e) {
      LOGGER.debug("Could not create scriptevaluator", e);
      throw new DbException("Could not create scriptevaluator", e);
    }
    se.setDefaultImports(MyriaConstants.DEFAULT_JANINO_IMPORTS);

    try {
      if (script.length() > 1) {
        return (ScriptEvalInterface) se.createFastEvaluator(script, ScriptEvalInterface.class, new String[] {
            Expression.INPUT, Expression.INPUTROW, Expression.STATE, Expression.STATEROW, Expression.STATECOLOFFSET });
      } else {
        return null;
      }
    } catch (CompileException e) {
      LOGGER.debug("Error when compiling expression {}: {}", script, e);
      throw new DbException("Error when compiling expression: " + script, e);
    }
  }

  @Override
  public Schema generateSchema(final Schema inputSchema) {
    Schema stateSchema = generateStateSchema(inputSchema);
    ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
    for (Expression expr : emitters) {
      typesBuilder.add(expr.getOutputType(new ExpressionOperatorParameter(inputSchema, stateSchema)));
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
