package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.io.DataSource;

public class DatasetEncoding extends MyriaApiEncoding {
  public RelationKey relationKey;
  public Schema schema;
  public String fileName;
  public Set<Integer> workers;
  public DataSource source;
  public Character delimiter;
  public Character escape;
  public Integer numberOfSkippedLines;
  public Character quote;
  public Boolean importFromDatabase = false;
  public List<List<IndexRef>> indexes;
  private static final List<String> requiredFields = ImmutableList.of("source", "relationKey", "schema");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }
}
