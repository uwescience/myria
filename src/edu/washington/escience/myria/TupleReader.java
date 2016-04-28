/**
 *
 */
package edu.washington.escience.myria;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "readerType")
@JsonSubTypes({ @Type(name = "Csv", value = CsvTupleReader.class) })
public interface TupleReader extends Serializable {

  void open(InputStream stream) throws IOException, DbException;

  Schema getSchema();

  TupleBatch readTuples() throws IOException, DbException;

  void done() throws IOException;
}
