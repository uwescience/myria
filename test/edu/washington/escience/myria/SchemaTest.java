package edu.washington.escience.myria;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class SchemaTest {

  @Test
  public void testConstructionWithNoNames() {
    List<Type> types = ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE);
    Schema schema = new Schema(types);
    assertEquals(types, schema.getColumnTypes());
    assertEquals(types.size(), schema.numColumns());
    List<String> names = ImmutableList.of("col0", "col1");
    assertEquals(names, schema.getColumnNames());
  }

  @Test
  public void testConstructionWithNames() {
    List<Type> types = ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE);
    List<String> names = ImmutableList.of("Mycol0", "Mycol1");
    Schema schema = new Schema(types, names);
    assertEquals(types, schema.getColumnTypes());
    assertEquals(types.size(), schema.numColumns());
    assertEquals(names, schema.getColumnNames());
  }

  @Test(expected = NullPointerException.class)
  public void testConstructionNullTypeInTypes() {
    List<Type> types = Lists.newLinkedList();
    types.add(Type.INT_TYPE);
    types.add(null);
    List<String> names = ImmutableList.of("Mycol0", "Mycol1");
    new Schema(types, names);
  }

  @Test(expected = NullPointerException.class)
  public void testConstructionNullNameInNames() {
    List<Type> types = ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE);
    List<String> names = Lists.newLinkedList();
    names.add(null);
    names.add("Mycol1");
    new Schema(types, names);
  }

  @Test
  public void testColumnPropertyGetters() {
    List<Type> types = ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE);
    List<String> names = ImmutableList.of("Mycol0", "Mycol1");
    Schema schema = new Schema(types, names);
    for (int i = 0; i < schema.numColumns(); ++i) {
      assertEquals(types.get(i), schema.getColumnType(i));
      assertEquals(names.get(i), schema.getColumnName(i));
    }
  }

  @Test
  public void testOfFields() {
    List<Type> types = ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE);
    List<String> names = ImmutableList.of("Mycol0", "Mycol1");
    Schema schema = new Schema(types, names);

    assertEquals(schema, Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "Mycol0", "Mycol1"));
    assertEquals(schema, Schema.ofFields(Type.INT_TYPE, "Mycol0", Type.LONG_TYPE, "Mycol1"));
    assertEquals(schema, Schema.ofFields(Type.INT_TYPE, "Mycol0", "Mycol1", Type.LONG_TYPE));
    assertEquals(schema, Schema.ofFields("Mycol0", Type.INT_TYPE, Type.LONG_TYPE, "Mycol1"));
    assertEquals(schema, Schema.ofFields("Mycol0", Type.INT_TYPE, "Mycol1", Type.LONG_TYPE));
    assertEquals(schema, Schema.ofFields("Mycol0", "Mycol1", Type.INT_TYPE, Type.LONG_TYPE));
  }

  @Test
  public void testOfFieldsNoNames() {
    List<Type> types = ImmutableList.of(Type.INT_TYPE, Type.LONG_TYPE);
    Schema schema = new Schema(types);
    assertEquals(schema, Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfFieldsTooFewNames() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "Mycol0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfFieldsTooManyNames() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "Mycol0", "Mycol1", "Mycol2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfFieldsNoTypes() {
    Schema.ofFields("Mycol0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOfFieldsBadType() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, 1, "Mycol0", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameWithSpace() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, " Mycol0", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameWithSpace2() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "Mycol0 ", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameWithSpace3() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, " Mycol0 ", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameWithSpace4() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, " ", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameStartsNumber() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "0col", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameHasHyphen() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "col-0", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameHasSymbol() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "col$0", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameEmpty() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "", "Mycol1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadNameHasParentheses() {
    Schema.ofFields(Type.INT_TYPE, Type.LONG_TYPE, "Mycol(0)", "Mycol1");
  }

  public void testCompatible() {
    assertTrue(Schema.ofFields("a", Type.INT_TYPE).compatible(Schema.ofFields("a", Type.INT_TYPE)));
    assertTrue(Schema.ofFields("a", Type.INT_TYPE).compatible(Schema.ofFields("b", Type.INT_TYPE)));
    assertTrue(Schema.ofFields("a", Type.INT_TYPE).compatible(Schema.ofFields(Type.INT_TYPE)));
    assertTrue(
        Schema.ofFields(Type.INT_TYPE, Type.DOUBLE_TYPE, Type.LONG_TYPE, Type.DATETIME_TYPE)
            .compatible(
                Schema.ofFields(
                    "a",
                    Type.INT_TYPE,
                    "b",
                    Type.DOUBLE_TYPE,
                    "c",
                    Type.LONG_TYPE,
                    "d",
                    Type.DATETIME_TYPE)));

    assertFalse(
        Schema.ofFields("a", Type.INT_TYPE).compatible(Schema.ofFields("a", Type.LONG_TYPE)));
    assertFalse(
        Schema.ofFields("a", Type.INT_TYPE)
            .compatible(Schema.ofFields("a", Type.INT_TYPE, "b", Type.LONG_TYPE)));
    assertFalse(
        Schema.ofFields(Type.INT_TYPE, Type.DOUBLE_TYPE, Type.LONG_TYPE, Type.DATETIME_TYPE)
            .compatible(
                Schema.ofFields(
                    Type.INT_TYPE, Type.DOUBLE_TYPE, Type.LONG_TYPE, Type.BOOLEAN_TYPE)));
  }
}
