package edu.washington.escience.myriad.api.encoding;

import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.api.MyriaApiException;

public class TipsyDatasetEncoding implements MyriaApiEncoding {
  public RelationKey relationKey;
  public String tipsyFilename;
  public String grpFilename;
  public String iorderFilename;
  public Set<Integer> workers;

  @Override
  public void validate() throws MyriaApiException {
    try {
      Preconditions.checkNotNull(relationKey);
      Preconditions.checkArgument(tipsyFilename != null);
      Preconditions.checkArgument(grpFilename != null);
      Preconditions.checkArgument(iorderFilename != null);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "required fields: relation_key, tipsy_filename, grp_filename, iorder_filename");
    }
  }
}