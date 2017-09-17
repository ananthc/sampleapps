/**
 * Put your copyright and license info here.
 */
package com.github.ananthc.dataworkssummit.kudu.output;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kudu.KuduExecutionContext;
import org.apache.apex.malhar.kudu.KuduMutationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is a simple operator that formats the incoming message that is compatible with Kudu execution context
 */
public class KafkaStreamInputOperator<T> extends AbstractKafkaInputOperator
{

  Class<T> clazz;

  public final transient DefaultOutputPort<KuduExecutionContext<T>> outputForWrites =
    new DefaultOutputPort<>();

  public KafkaStreamInputOperator(Class clazz)
  {
    this.clazz = clazz;
  }

  public KafkaStreamInputOperator()
  {
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(KafkaStreamInputOperator.class);

  private ObjectMapper objectMapper = null;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    objectMapper = new ObjectMapper();
  }

  @Override
  protected void emitTuple(String clusterName, ConsumerRecord<byte[], byte[]> consumerRecord)
  {
    T payload = null;
    try {
      payload = objectMapper.readValue(consumerRecord.value(),clazz);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    if (payload == null) {
      LOG.error("Null payload");
      return;
    }
    KuduExecutionContext<T> executionContext = new KuduExecutionContext<>();
    executionContext.setPayload(payload);
    executionContext.setMutationType(KuduMutationType.UPSERT);
    outputForWrites.emit(executionContext);
  }


}
