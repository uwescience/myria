package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;

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
import edu.washington.escience.myria.expression.DivideExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.PowExpression;
import edu.washington.escience.myria.expression.SubstrExpression;
import edu.washington.escience.myria.expression.WorkerIdExpression;
import edu.washington.escience.myria.expression.evaluate.SqlExpressionOperatorParameter;
import edu.washington.escience.myria.expression.sql.ColumnReferenceExpression;
import edu.washington.escience.myria.expression.sql.SqlQuery;

public class QueryScanTest {

  @Test
  public void testSelectGeneration() throws DbException {
    RelationKey r = new RelationKey("public", "adhoc", "R");
    RelationKey s = new RelationKey("public", "adhoc", "S");
    ColumnReferenceExpression x = new ColumnReferenceExpression(r, 0);
    ColumnReferenceExpression y = new ColumnReferenceExpression(r, 1);
    ColumnReferenceExpression z = new ColumnReferenceExpression(s, 0);
    Expression xe = new Expression("x", x);
    Expression ye = new Expression("y", y);
    Expression ze = new Expression("z", z);
    ExpressionOperator w = new AndExpression(new LessThanExpression(x, y), new EqualsExpression(x, z));

    LinkedHashMap<RelationKey, Schema> schemas = Maps.newLinkedHashMap();
    schemas.put(r, Schema
        .of(ImmutableList.<Type> of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.<String> of("x", "y")));
    schemas.put(s, Schema.of(ImmutableList.<Type> of(Type.INT_TYPE), ImmutableList.<String> of("z")));

    SqlExpressionOperatorParameter params =
        new SqlExpressionOperatorParameter(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, -1);

    // spj query
    {
      SqlQuery query = new SqlQuery(ImmutableList.<Expression> of(xe, ye, ze), schemas, w, null, null);
      assertEquals(
          query.getSqlString(params),
          "SELECT rel0.x,rel0.y,rel1.z\nFROM \"public adhoc R\" AS rel0,\"public adhoc S\" AS rel1\nWHERE ((rel0.x<rel0.y) AND (rel0.x=rel1.z))");

      assertEquals(query.getOutputSchema(params), Schema.of(ImmutableList.<Type> of(Type.INT_TYPE, Type.INT_TYPE,
          Type.INT_TYPE), ImmutableList.<String> of("x", "y", "z")));
    }

    // select *
    {
      SqlQuery query = new SqlQuery(r);
      assertEquals(query.getSqlString(params), "SELECT *\nFROM \"public adhoc R\" AS rel0");
    }

    // order by
    {
      SqlQuery query =
          new SqlQuery(ImmutableList.<Expression> of(xe, ye), schemas, null, ImmutableList
              .<ColumnReferenceExpression> of(x, y), ImmutableList.<Boolean> of(true, false));
      assertEquals(query.getSqlString(params),
          "SELECT rel0.x,rel0.y\nFROM \"public adhoc R\" AS rel0,\"public adhoc S\" AS rel1\nORDER BY rel0.x ASC,rel0.y DESC");
    }
  }

  @Test
  public void testSqlGeneration() throws DbException {
    RelationKey r = new RelationKey("public", "adhoc", "R");
    RelationKey s = new RelationKey("public", "adhoc", "S");
    ExpressionOperator x = new ColumnReferenceExpression(r, 0);
    ExpressionOperator y = new ColumnReferenceExpression(r, 1);
    ExpressionOperator z = new ColumnReferenceExpression(s, 0);

    HashMap<RelationKey, Schema> schemas = Maps.newLinkedHashMap();
    schemas.put(r, Schema
        .of(ImmutableList.<Type> of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.<String> of("x", "y")));
    schemas.put(s, Schema.of(ImmutableList.<Type> of(Type.INT_TYPE), ImmutableList.<String> of("z")));

    SqlExpressionOperatorParameter params =
        new SqlExpressionOperatorParameter(schemas, MyriaConstants.STORAGE_SYSTEM_POSTGRESQL, 42);

    params.generateAliases(ImmutableList.of(r, s));

    {
      ExpressionOperator and = new AndExpression(new LessThanExpression(x, y), new EqualsExpression(x, z));
      assertEquals("((rel0.x<rel0.y) AND (rel0.x=rel1.z))", and.getSqlString(params));
    }

    {
      ExpressionOperator expr =
          new AndExpression(new PowExpression(x, y), new PlusExpression(z, new MinusExpression(x, y)));
      assertEquals("(power(rel0.x,rel0.y) AND (rel1.z+(rel0.x-rel0.y)))", expr.getSqlString(params));
    }

    {
      ExpressionOperator expr = new WorkerIdExpression();
      assertEquals("42", expr.getSqlString(params));
    }

    {
      ExpressionOperator expr = new DivideExpression(new ConstantExpression(4), y);
      assertEquals("(float(4)/rel0.y)", expr.getSqlString(params));
    }

    {
      ExpressionOperator expr = new PlusExpression(new ConstantExpression(0.5), new ConstantExpression(true));
      assertEquals("(0.5+true)", expr.getSqlString(params));
    }

    {
      ExpressionOperator expr =
          new SubstrExpression(new ConstantExpression("Hello World"), new ConstantExpression(3),
              new ConstantExpression(5));
      assertEquals("substring('Hello World' from 3 for (5)-(3))", expr.getSqlString(params));
    }
  }
}
