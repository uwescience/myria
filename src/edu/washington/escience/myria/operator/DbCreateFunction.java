package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FunctionLanguage;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.storage.TupleBatch;
/**
 *
 *Class for creating user defined functions.
 *
 */
public class DbCreateFunction extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** function name.*/
  private final String name;
  /** function body.*/
  private final String binary;
  /** function description or text.*/
  private final String description;
  /** does function return multiple tuples.*/
  private final Boolean isMultivalued;
  /** function language.*/
  private final MyriaConstants.FunctionLanguage lang;
  /** function output schema.*/
  private final String outputType;

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(GenericEvaluator.class);

  /**
   * @param child the source of tuples to be inserted.
   * @param name function name.
   * @param connectionInfo the parameters of the database connection.
   * @param outputType output schema for the function
   * @param isMultivalued does it return multiple tuples?
   * @param lang function type
   * @param binary function body (encoded binary string)
   * @param description function decription, this is kept in the catalog and not sent to workers.
   */
  public DbCreateFunction(
      final Operator child,
      final String name,
      final String description,
      final String outputType,
      final Boolean isMultivalued,
      final FunctionLanguage lang,
      final String binary,
      final ConnectionInfo connectionInfo) {
    super(child);
    this.name = name;
    this.description = description;
    this.outputType = outputType;
    this.isMultivalued = isMultivalued;
    this.connectionInfo = connectionInfo;
    this.lang = lang;
    this.binary = binary;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars)
      throws DbException, IOException {
    /* Retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }
    switch (lang) {
      case POSTGRES:
        Pattern pattern = Pattern.compile("(CREATE FUNCTION)([\\s\\S]*)(LANGUAGE SQL;)");
        Matcher matcher = pattern.matcher(description);

        if (matcher.matches()) {
          /* Add a replace statement */
          String modifiedReplaceFunction =
              description.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION");

          /* Run command */
          accessMethod.runCommand(modifiedReplaceFunction);
        } else {
          throw new DbException("Postgres function is invalid.");
        }

        break;
      case PYTHON:
        if (binary != null) {
          PythonFunctionRegistrar pyFunc = new PythonFunctionRegistrar(connectionInfo);
          pyFunc.addFunction(name, description, outputType, isMultivalued, binary);
        } else {
          throw new DbException("Cannot register python UDF without binary.");
        }
        break;
      default:
        throw new DbException("Function language not supported!");
    }
  }

  @Override
  public void cleanup() {
    try {
      if (accessMethod != null) {
        accessMethod.close();
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tuples) throws DbException {}

  @Override
  protected void childEOS() throws DbException {}

  @Override
  protected void childEOI() throws DbException {}
}
