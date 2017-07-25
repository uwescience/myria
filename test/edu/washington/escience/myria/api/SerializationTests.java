package edu.washington.escience.myria.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.HashDistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.RoundRobinDistributeFunction;

public class SerializationTests {

  public static ObjectMapper mapper;

  @BeforeClass
  public static void setUp() {
    mapper = MyriaJsonMapperProvider.getMapper();
  }

  @Test
  public void testDistributeFunction() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(DistributeFunction.class);
    String serialized;
    DistributeFunction deserialized;

    /* Multi-field hash */
    int multiFieldIndex[] = new int[] {3, 4, 2};
    DistributeFunction df = new HashDistributeFunction(multiFieldIndex);
    serialized = mapper.writeValueAsString(df);
    deserialized = reader.readValue(serialized);
    assertEquals(df.getClass(), deserialized.getClass());
    HashDistributeFunction pfMFH = (HashDistributeFunction) deserialized;
    assertArrayEquals(multiFieldIndex, pfMFH.getIndexes());

    /* RoundRobin */
    df = new RoundRobinDistributeFunction();
    serialized = mapper.writeValueAsString(df);
    deserialized = reader.readValue(serialized);
    assertEquals(df.getClass(), deserialized.getClass());
  }

  @Test
  public void testDistributeFunctionWithNullNumPartitions() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(DistributeFunction.class);
    HashDistributeFunction df = new HashDistributeFunction(new int[] {3});
    String serialized = mapper.writeValueAsString(df);
    DistributeFunction deserialized = reader.readValue(serialized);
    assertEquals(df.getClass(), deserialized.getClass());
    assertEquals(3, ((HashDistributeFunction) deserialized).getIndexes()[0]);
  }

  @Test
  public void testBlobConstantExpression() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(ConstantExpression.class);
    byte[] bytes = {0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F};
    ConstantExpression expr = new ConstantExpression(ByteBuffer.wrap(bytes));
    assertEquals("CgsMDQ4P", expr.getValue());
    String serialized = mapper.writeValueAsString(expr);
    ConstantExpression deserialized = reader.readValue(serialized);
    assertEquals(expr.getClass(), deserialized.getClass());
    assertEquals("CgsMDQ4P", deserialized.getValue());
  }
}
