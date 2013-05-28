package edu.washington.escience.myriad.api.encoding;

import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.api.MyriaApiException;
import edu.washington.escience.myriad.operator.Operator;

/**
 * A JSON-able wrapper for the expected wire message for an operator. To add a new operator, three things need to be
 * done.
 * 
 * 1. Create an Encoding class that extends OperatorEncoding.
 * 
 * 2. Add the operator to the list of JsonSubTypes below.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "op_type")
@JsonSubTypes({
    @Type(name = "CollectConsumer", value = CollectConsumerEncoding.class),
    @Type(name = "CollectProducer", value = CollectProducerEncoding.class),
    @Type(name = "Consumer", value = ConsumerEncoding.class),
    @Type(name = "LocalMultiwayConsumer", value = LocalMultiwayConsumerEncoding.class),
    @Type(name = "ShuffleConsumer", value = ShuffleConsumerEncoding.class),
    @Type(name = "ShuffleProducer", value = ShuffleProducerEncoding.class),
    @Type(name = "LocalMultiwayProducer", value = LocalMultiwayProducerEncoding.class),
    @Type(name = "LocalCountingJoin", value = LocalCountingJoinEncoding.class),
    @Type(name = "LocalJoin", value = LocalJoinEncoding.class),
    @Type(name = "SQLiteInsert", value = SQLiteInsertEncoding.class),
    @Type(name = "EOSController", value = EOSControllerEncoding.class),
    @Type(name = "IDBInput", value = IDBInputEncoding.class),
    @Type(name = "Aggregate", value = AggregateEncoding.class),
    @Type(name = "SQLiteScan", value = SQLiteScanEncoding.class) })
public abstract class OperatorEncoding<T extends Operator> implements MyriaApiEncoding {

  public String opName;
  public String opType;

  @Override
  public void validate() throws MyriaApiException {
    try {
      Preconditions.checkNotNull(opName);
      Preconditions.checkNotNull(opType);
    } catch (Exception e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "required fields: op_name, op_type");
    }
  }

  /**
   * @return an instantiated operator.
   */
  public abstract T construct();

  /**
   * Connect any operators to this one.
   */
  public abstract void connect(Operator operator, Map<String, Operator> operators);
}