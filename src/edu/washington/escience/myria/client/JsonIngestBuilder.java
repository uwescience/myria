package edu.washington.escience.myria.client;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.DatasetEncoding;
import edu.washington.escience.myria.io.ByteArraySource;
import edu.washington.escience.myria.io.FileSource;

/**
 * Json dataset ingest builder base. This class provides common building blocks for creating json dataset ingest.
 * 
 * */
public final class JsonIngestBuilder {

  /**
   * Ingest a data array.
   * 
   * @param data data
   * @param dataSchema the schema
   * @param ingestInto where the data should be ingested
   * @return the encoding of the data set ingest API call this builder is going to build.
   * */
  public DatasetEncoding buildIngest(final byte[] data, final Schema dataSchema, final RelationKey ingestInto) {
    Preconditions.checkNotNull(data);
    DatasetEncoding de = new DatasetEncoding();
    de.source = new ByteArraySource(data);
    de.delimiter = null;
    de.importFromDatabase = false;
    de.relationKey = ingestInto;
    de.schema = dataSchema;

    return de;
  }

  /**
   * Ingest a data array.
   * 
   * @param data data
   * @param dataSchema the schema
   * @param ingestInto where the data should be ingested
   * @return the Json encoding of the data set ingest API call this builder is going to build.
   * */
  public String buildJson(final byte[] data, final Schema dataSchema, final RelationKey ingestInto) {
    return toJson(buildIngest(data, dataSchema, ingestInto));
  }

  /**
   * Ingest a file.
   * 
   * @param filename .
   * @param delimiter .
   * @param dataSchema the schema
   * @param ingestInto where the data should be ingested
   * @return the encoding of the data set ingest API call this builder is going to build.
   * */
  public DatasetEncoding buildIngest(final String filename, final String delimiter, final Schema dataSchema,
      final RelationKey ingestInto) {

    DatasetEncoding de = new DatasetEncoding();
    de.source = new FileSource(filename);
    de.delimiter = delimiter;
    de.importFromDatabase = false;
    de.relationKey = ingestInto;
    de.schema = dataSchema;

    return de;
  }

  /**
   * Ingest a file.
   * 
   * @param filename .
   * @param delimiter .
   * @param dataSchema the schema
   * @param ingestInto where the data should be ingested
   * @return the Json encoding of the data set ingest API call this builder is going to build.
   * */
  public String buildJson(final String filename, final String delimiter, final Schema dataSchema,
      final RelationKey ingestInto) {
    return toJson(buildIngest(filename, delimiter, dataSchema, ingestInto));
  }

  /**
   * Ingest an existing data set.
   * 
   * @param ingestInto where the data should be ingested
   * @return the encoding of the data set ingest API call this builder is going to build.
   * */
  public DatasetEncoding buildIngest(final RelationKey ingestInto) {

    DatasetEncoding de = new DatasetEncoding();
    de.source = null;
    de.delimiter = null;
    de.importFromDatabase = true;
    de.relationKey = ingestInto;
    de.schema = null;

    return de;
  }

  /**
   * Ingest an existing data set.
   * 
   * @param ingestInto where the data should be ingested
   * @return the Json encoding of the data set ingest API call this builder is going to build.
   * */
  public String buildJson(final RelationKey ingestInto) {
    return toJson(buildIngest(ingestInto));
  }

  /**
   * Convert a DatasetEncoding to Json.
   * 
   * @param de .
   * @return Json
   * */
  public static String toJson(final DatasetEncoding de) {
    try {
      de.validate();
    } catch (MyriaApiException e) {
      throw new IllegalArgumentException("Invalid dataset ingest. Cause: " + e.getResponse().getEntity().toString(), e
          .getCause());
    }

    ObjectMapper ow = MyriaJsonMapperProvider.newMapper();
    try {
      return ow.writerWithDefaultPrettyPrinter().writeValueAsString(de);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
