package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.util.FSUtils;

public class DatasetEncoding extends MyriaApiEncoding {
  public RelationKey relationKey;
  public Schema schema;
  public String fileName;
  public Set<Integer> workers;
  public Boolean isCommaSeparated;
  public byte[] data;
  private static final List<String> requiredFields = ImmutableList.of("relationKey", "schema");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  @Override
  protected void validateExtra() throws MyriaApiException {
    try {
      Preconditions.checkArgument(fileName != null || data != null);
    } catch (final Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, getClass().getName()
          + " has required fields: relation_key, schema, file_name | data");
    }
    /*
     * Note we can only do this because we know that the operator will be run on the master. So we can't do this e.g.
     * for FileScan because that might be run on a worker.
     */
    if (null == data) {
      try {
        FSUtils.checkFileReadable(fileName);
      } catch (Exception e) {
        throw new MyriaApiException(Status.BAD_REQUEST, e);
      }
    }
  }
}