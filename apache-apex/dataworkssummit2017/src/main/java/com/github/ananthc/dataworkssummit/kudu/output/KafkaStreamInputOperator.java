/**
 * Put your copyright and license info here.
 */
package com.github.ananthc.dataworkssummit.kudu.output;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kudu.KuduExecutionContext;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is a simple operator that formats the incoming message that is compatible with Kudu execution context
 */
public class KafkaStreamInputOperator extends AbstractKafkaInputOperator
{

  public final transient DefaultOutputPort<KuduExecutionContext<FiftyColsPojo>> outputFor50ColTransactionWrites =
    new DefaultOutputPort<>();

  public final transient DefaultOutputPort<KuduExecutionContext<FiftyColsPojo>> outputFor25ColTransactionWrites =
    new DefaultOutputPort<>();

  public final transient DefaultOutputPort<KuduExecutionContext<FiftyColsPojo>> outputFor100ColTransactionWrites =
    new DefaultOutputPort<>();


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
    TransactionPayload payload = null;
    try {
      payload = objectMapper.readValue(consumerRecord.value(),TransactionPayload.class);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    if (payload == null) {
      return;
    }
    if (payload.getTransactionEditMode() == 'i') {
      outputForTransactionWrites.emit(resolveExecutionContextForTransactTable(payload));
    }
    if (payload.getTransactionEditMode() == 'u') {
      outputForTransactionWrites.emit(resolveExecutionContextForUpdate(payload));
    }
    outputForDeviceWrites.emit(resolveExecutionContextForDevicesTable(payload));
  }


}
