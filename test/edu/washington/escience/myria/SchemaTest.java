package edu.washington.escience.myria;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

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
}
