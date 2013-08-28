package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class DbInsertEncoding extends OperatorEncoding<DbInsert> {
  /** The name under which the dataset will be stored. */
  public RelationKey relationKey;
  /** The source of tuples to be inserted. */
  public String argChild;
  /** Whether to overwrite an existing dataset. */
  public Boolean argOverwriteTable;
  /**
   * The ConnectionInfo struct determines what database the data will be written to. If null, the worker's default
   * database will be used.
   */
  public ConnectionInfo connectionInfo;
  private static final List<String> requiredArguments = ImmutableList.of("relationKey", "argChild");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public DbInsert construct(Server server) {
    if (argOverwriteTable == null) {
      argOverwriteTable = Boolean.FALSE;
    }
    return new DbInsert(null, relationKey, connectionInfo, argOverwriteTable);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}