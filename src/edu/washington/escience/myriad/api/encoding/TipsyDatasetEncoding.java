package edu.washington.escience.myriad.api.encoding;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;

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
}