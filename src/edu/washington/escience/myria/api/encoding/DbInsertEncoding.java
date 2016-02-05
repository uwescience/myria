package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.base.MoreObjects;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.io.UriSink;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import org.apache.hadoop.metrics2.sink.FileSink;

/**
 * A JSON-able wrapper for the expected wire message for a new dataset.
 * 
 */
public class DbInsertEncoding extends UnaryOperatorEncoding<DataOutput> {
  /** The name under which the dataset will be stored. */
  @Required
  public RelationKey relationKey;
  /** Whether to overwrite an existing dataset. */
  public Boolean argOverwriteTable;
  /** Indexes created. */
  public List<List<IndexRef>> indexes;
  /** The PartitionFunction used to partition this relation. */
  public PartitionFunction partitionFunction;

  /**
   * The ConnectionInfo struct determines what database the data will be written to. If null, the worker's default
   * database will be used.
   */
  public ConnectionInfo connectionInfo;

  @Override
  public DataOutput construct(final ConstructArgs args) {
    /* default overwrite to {@code false}, so we append. */
    argOverwriteTable = MoreObjects.firstNonNull(argOverwriteTable, Boolean.FALSE);
    TupleWriter tupleWriter = new CsvTupleWriter();
    try {
      DataSink dataSink = new UriSink("file:///tmp/foo");
      return new DataOutput(null, tupleWriter, dataSink);
      //return new DbInsert(null, relationKey, connectionInfo, argOverwriteTable, indexes, partitionFunction);
    } catch(CatalogException e) {
      throw new RuntimeException(e);
    }
  }
}
