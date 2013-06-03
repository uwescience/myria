package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class SQLiteInsertEncoding extends OperatorEncoding<SQLiteInsert> {
  /** The name under which the dataset will be stored. */
  public RelationKey relationKey;
  /** The source of tuples to be inserted. */
  public String argChild;
  /** Whether to overwrite an existing dataset. */
  public Boolean argOverwriteTable;
  private static final List<String> requiredArguments = ImmutableList.of("relationKey", "argChild");

  @Override
  public void connect(final Operator current, final Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public SQLiteInsert construct() {
    if (argOverwriteTable != null) {
      return new SQLiteInsert(null, relationKey, argOverwriteTable);
    }
    return new SQLiteInsert(null, relationKey);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}