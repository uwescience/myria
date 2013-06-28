package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.util.FSUtils;

public class TipsyDatasetEncoding extends MyriaApiEncoding {
  public RelationKey relationKey;
  public String tipsyFilename;
  public String grpFilename;
  public String iorderFilename;
  public Set<Integer> workers;
  private static final List<String> requiredFields = ImmutableList.of("relationKey", "tipsyFilename", "grpFilename",
      "iorderFilename");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  @Override
  public void validateExtra() {
    /*
     * Note we can only do this because we know that the operator will be run on the master. So we can't do this e.g.
     * for TipsyFileScan because that might be run on a worker.
     */
    try {
      FSUtils.checkFileReadable(tipsyFilename);
      FSUtils.checkFileReadable(grpFilename);
      FSUtils.checkFileReadable(iorderFilename);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }
  }
}