package edu.washington.escience.myria.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import edu.washington.escience.myria.operator.network.partition.MultiFieldHashPartitionFunction;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.operator.network.partition.WholeTupleHashPartitionFunction;

public class SerializationTests {

  public static ObjectMapper mapper;

  @BeforeClass
  public static void setUp() {
    mapper = MyriaJsonMapperProvider.getMapper();
  }

  @Test
  public void testPartitionFunction() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(PartitionFunction.class);
    PartitionFunction pf;
    String serialized;
    PartitionFunction deserialized;

    /* Single field hash */
    pf = new SingleFieldHashPartitionFunction(5, 3);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());
    SingleFieldHashPartitionFunction pfSFH = (SingleFieldHashPartitionFunction) deserialized;
    assertEquals(3, pfSFH.getIndex());

    /* Multi-field hash */
    int multiFieldIndex[] = new int[] {3, 4, 2};
    pf = new MultiFieldHashPartitionFunction(5, multiFieldIndex);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());
    MultiFieldHashPartitionFunction pfMFH = (MultiFieldHashPartitionFunction) deserialized;
    assertArrayEquals(multiFieldIndex, pfMFH.getIndexes());

    /* Whole tuple hash */
    pf = new WholeTupleHashPartitionFunction(5);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());

    /* RoundRobin */
    pf = new RoundRobinPartitionFunction(5);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(5, deserialized.numPartition());
  }

  @Test
  public void testPartitionFunctionWithNullNumPartitions() throws Exception {
    /* Setup */
    ObjectReader reader = mapper.reader(PartitionFunction.class);
    PartitionFunction pf;
    String serialized;
    PartitionFunction deserialized;

    /* Single field hash, as one representative */
    pf = new SingleFieldHashPartitionFunction(null, 3);
    serialized = mapper.writeValueAsString(pf);
    deserialized = reader.readValue(serialized);
    assertEquals(pf.getClass(), deserialized.getClass());
    assertEquals(3, ((SingleFieldHashPartitionFunction) deserialized).getIndex());
  }
}
