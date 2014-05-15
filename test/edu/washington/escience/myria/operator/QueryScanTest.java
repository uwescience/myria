package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.AndExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.PowExpression;
import edu.washington.escience.myria.expression.WorkerIdExpression;
import edu.washington.escience.myria.expression.evaluate.SqlExpressionOperatorParameter;
import edu.washington.escience.myria.expression.sql.ColumnReferenceExpression;
import edu.washington.escience.myria.expression.sql.SelectOperator;

public class QueryScanTest {

  @Test
  public void testSelectGeneration() throws DbException {
    RelationKey r = new RelationKey("public", "adhoc", "R");
    RelationKey s = new RelationKey("public", "adhoc", "S");
    ExpressionOperator x = new ColumnReferenceExpression(r, 0);
    ExpressionOperator y = new ColumnReferenceExpression(r, 1);
    ExpressionOperator z = new ColumnReferenceExpression(s, 0);
    ExpressionOperator w = new AndExpression(new LessThanExpression(x, y), new EqualsExpression(x, z));

    HashMap<RelationKey, Schema> schemas = Maps.newHashMap();
    schemas.put(r, Schema
        .of(ImmutableList.<Type> of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.<String> of("x", "y")));
    schemas.put(s, Schema.of(ImmutableList.<Type> of(Type.INT_TYPE), ImmutableList.<String> of("z")));

    SqlExpressionOperatorParameter params =
        new SqlExpressionOperatorParameter(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, -1);

    SelectOperator select =
        new SelectOperator(ImmutableList.<ExpressionOperator> of(x, y, z), ImmutableList.<RelationKey> of(r, s), w);
    select.setSchemas(schemas);
    assertEquals(
        select.getSqlString(params),
        "SELECT rel0.x,rel0.y,rel1.z\nFROM \"public adhoc R\" AS rel0,\"public adhoc S\" AS rel1\nWHERE ((rel0.x<rel0.y) AND (rel0.x=rel1.z))");

    SelectOperator selectStar = new SelectOperator(ImmutableList.<RelationKey> of(r));
    select.setSchemas(schemas);
    assertEquals(selectStar.getSqlString(params), "SELECT *\nFROM \"public adhoc R\" AS rel0");
  }

  @Test
  public void testSqlGeneration() throws DbException {
    RelationKey r = new RelationKey("public", "adhoc", "R");
    RelationKey s = new RelationKey("public", "adhoc", "S");
    ExpressionOperator x = new ColumnReferenceExpression(r, 0);
    ExpressionOperator y = new ColumnReferenceExpression(r, 1);
    ExpressionOperator z = new ColumnReferenceExpression(s, 0);

    HashMap<RelationKey, Schema> schemas = Maps.newHashMap();
    schemas.put(r, Schema
        .of(ImmutableList.<Type> of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.<String> of("x", "y")));
    schemas.put(s, Schema.of(ImmutableList.<Type> of(Type.INT_TYPE), ImmutableList.<String> of("z")));

    SqlExpressionOperatorParameter params =
        new SqlExpressionOperatorParameter(schemas, MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, 42);

    params.generateAliases(ImmutableList.of(r, s));

    ExpressionOperator and = new AndExpression(new LessThanExpression(x, y), new EqualsExpression(x, z));
    assertEquals(and.getSqlString(params), "((rel0.x<rel0.y) AND (rel0.x=rel1.z))");

    ExpressionOperator complex =
        new AndExpression(new PowExpression(x, y), new PlusExpression(z, new MinusExpression(x, y)));
    assertEquals(complex.getSqlString(params), "(power(rel0.x,rel0.y) AND (rel1.z+(rel0.x-rel0.y)))");

    ExpressionOperator worker = new WorkerIdExpression();
    assertEquals(worker.getSqlString(params), "42");

    ExpressionOperator constants = new PlusExpression(new ConstantExpression(0.5), new ConstantExpression(true));
    assertEquals(constants.getSqlString(params), "(0.5+true)");

  }
}
