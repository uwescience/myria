package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;

public class DatasetEncoding extends MyriaApiEncoding {
  public RelationKey relationKey;
  public Schema schema;
  public String fileName;
  public Set<Integer> workers;
  public Boolean isCommaSeparated;
  public byte[] data;
  public Boolean importFromDatabase = false;
  private static final List<String> requiredFields = ImmutableList.of("relationKey", "schema");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  @Override
  protected void validateExtra() throws MyriaApiException {
    try {
      Preconditions.checkArgument(fileName != null || data != null || importFromDatabase);
    } catch (final Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, getClass().getName()
          + " has required fields: relation_key, schema, file_name | data");
    }
  }
}
