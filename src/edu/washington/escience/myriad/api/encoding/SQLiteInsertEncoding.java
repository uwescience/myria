package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class SQLiteInsertEncoding extends OperatorEncoding<SQLiteInsert> {
  /** The name of the dataset to be scanned. */
  public RelationKey relationKey;
  public String argChild;
  public Boolean argOverwriteTable;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(relationKey);
      Preconditions.checkNotNull(argChild);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: relation_key");
    }
  }

  @Override
  public SQLiteInsert construct() {
    if (argOverwriteTable != null) {
      return new SQLiteInsert(null, relationKey, argOverwriteTable);
    }
    return new SQLiteInsert(null, relationKey);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}