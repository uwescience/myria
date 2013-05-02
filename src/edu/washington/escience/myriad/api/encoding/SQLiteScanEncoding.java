package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.api.MyriaApiUtils;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class SQLiteScanEncoding extends OperatorEncoding<SQLiteQueryScan> {
  /** The name of the dataset to be scanned. */
  public RelationKey relationKey;

  @Override
  public void validate() throws MyriaApiException {
    super.validate();
    try {
      Preconditions.checkNotNull(relationKey);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: relation_key");
    }
  }

  @Override
  public SQLiteQueryScan construct() {
    Schema schema;
    try {
      schema = MyriaApiUtils.getServer().getSchema(relationKey);
    } catch (final CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    if (schema == null) {
      throw new MyriaApiException(Status.BAD_REQUEST, "Specified relation "
          + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " does not exist.");
    }
    return new SQLiteQueryScan("SELECT * from " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE), schema);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    /* Do nothing; no children. */
  }
}