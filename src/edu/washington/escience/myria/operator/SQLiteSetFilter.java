package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * This operator implements set selection from a underlying database.<br>
 * It uses the SQL statement: select xxx, yyy, zzz,... from ttt where column in (a,b,c,d,e....).
 *
 * Note that the result of the child must be a set. Otherwise the result may have duplicates.
 * */
public class SQLiteSetFilter extends UnaryOperator {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * SQL template.
   * */
  private final String sqlTemplate;

  /**
   * Output schema.
   * */
  private final Schema outputSchema;

  /**
   * Iterator over a resultset of a SQLite query.
   * */
  private transient Iterator<TupleBatch> tuples;

  /**
   * SQLite connection information.
   * */
  private transient SQLiteInfo sqliteInfo;

  /**
   * @param child child.
   * @param tableName the table to query from.
   * @param setColumnName the column to compare against.
   * @param resultColumnNames the columns to put into the result.
   * @param outputSchema output schema.
   * */
  public SQLiteSetFilter(
      final Operator child,
      final String tableName,
      final String setColumnName,
      final String[] resultColumnNames,
      final Schema outputSchema) {
    super(child);
    Preconditions.checkArgument(child.getSchema().numColumns() == 1);
    sqlTemplate =
        String.format(
            "select %s from %s where %s in ( ",
            StringUtils.join(resultColumnNames, ","),
            tableName,
            setColumnName);
    this.outputSchema = outputSchema;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    sqliteInfo = (SQLiteInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    if (sqliteInfo == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
    tuples = null;
  }

  @Override
  protected final void cleanup() throws DbException {
    tuples = null;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (tuples != null) {
      if (tuples.hasNext()) {
        return tuples.next();
      }
      tuples = null;
    }
    final Operator child = getChild();

    TupleBatch tb = child.nextReady();
    if (tb != null) {
      int numTuples = tb.numTuples();
      Type ft = child.getSchema().getColumnType(0);
      ArrayList<Object> setValues = new ArrayList<Object>();
      Object v;
      for (int i = 0; i < numTuples; i++) {
        switch (ft) {
          case STRING_TYPE:
            v = "'" + tb.getString(0, i) + "'";
            break;
          case DATETIME_TYPE:
            v = "'" + tb.getDateTime(0, i) + "'";
            break;
          default:
            v = tb.getObject(0, i);
        }
        setValues.add(v);
      }
      tuples =
          SQLiteAccessMethod.tupleBatchIteratorFromQuery(
              sqliteInfo, sqlTemplate + StringUtils.join(setValues, ",") + ")", outputSchema);
      if (tuples.hasNext()) {
        return tuples.next();
      }
      tuples = null;
      return null;
    } else {
      return null;
    }
  }

  @Override
  protected final Schema generateSchema() {
    return outputSchema;
  }
}
