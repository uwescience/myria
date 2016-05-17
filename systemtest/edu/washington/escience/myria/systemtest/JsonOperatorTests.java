package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.HttpURLConnection;
import java.nio.file.Paths;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.CrossWithSingletonEncoding;
import edu.washington.escience.myria.api.encoding.DbInsertEncoding;
import edu.washington.escience.myria.api.encoding.EmptyRelationEncoding;
import edu.washington.escience.myria.api.encoding.FileScanEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.api.encoding.SingletonEncoding;
import edu.washington.escience.myria.api.encoding.SplitEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubQueryEncoding;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * System tests of operators using plans submitted via JSON. Tests both the API encoding of the operator AND the
 * serializability of the operator.
 */
public class JsonOperatorTests extends SystemTestBase {

  static Logger LOGGER = LoggerFactory.getLogger(JsonOperatorTests.class);

  @Test
  public void crossWithSingletonTest() throws Exception {
    SingletonEncoding singleton = new SingletonEncoding();
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    CrossWithSingletonEncoding cross = new CrossWithSingletonEncoding();
    DbInsertEncoding insert = new DbInsertEncoding();

    RelationKey outputRelation = RelationKey.of("test", "crosswithsingleton", "empty");
    singleton.opId = 0;
    empty.opId = 1;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    cross.opId = 2;
    cross.argChild1 = empty.opId;
    cross.argChild2 = singleton.opId;
    insert.opId = 3;
    insert.argChild = cross.opId;
    insert.relationKey = outputRelation;
    insert.argOverwriteTable = true;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(singleton, empty, cross, insert);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "CrossWithSingleton test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
  }

  @Test
  public void splitTest() throws Exception {
    File currentDir = new File(".");
    DataSource source =
        new FileSource(
            Paths.get(
                    currentDir.getAbsolutePath(),
                    "testdata",
                    "filescan",
                    "one_col_string_array.txt")
                .toString());

    Schema schema = Schema.of(ImmutableList.of(Type.STRING_TYPE), ImmutableList.of("string_array"));
    FileScanEncoding fs = new FileScanEncoding();
    fs.source = source;
    fs.schema = schema;
    fs.delimiter = ',';
    fs.quote = null;
    fs.escape = null;
    fs.skip = null;
    fs.opId = 0;
    SplitEncoding split = new SplitEncoding();
    split.splitColumnIndex = 0;
    split.regex = ":";
    split.argChild = fs.opId;
    split.opId = 1;
    RelationKey outputRelation = RelationKey.of("test", "split", "output");
    DbInsertEncoding insert = new DbInsertEncoding();
    insert.opId = 2;
    insert.argChild = split.opId;
    insert.relationKey = outputRelation;
    insert.argOverwriteTable = true;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(fs, split, insert);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Split test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);

    String data =
        JsonAPIUtils.download(
            "localhost",
            masterDaemonPort,
            outputRelation.getUserName(),
            outputRelation.getProgramName(),
            outputRelation.getRelationName(),
            "json");
    String expectedData =
        "[{\"string_array\":\"a:b:c:d:e:f\",\"string_array_splits\":\"a\"},{\"string_array\":\"a:b:c:d:e:f\",\"string_array_splits\":\"b\"},{\"string_array\":\"a:b:c:d:e:f\",\"string_array_splits\":\"c\"},{\"string_array\":\"a:b:c:d:e:f\",\"string_array_splits\":\"d\"},{\"string_array\":\"a:b:c:d:e:f\",\"string_array_splits\":\"e\"},{\"string_array\":\"a:b:c:d:e:f\",\"string_array_splits\":\"f\"}]";
    assertEquals(data, expectedData);
  }
}
