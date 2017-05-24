/**
 * Put your copyright and license info here.
 */
package github.ananthc.sampleapps.apex.kuduoutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.kudu.KuduExecutionContext;
import org.apache.apex.malhar.contrib.kudu.KuduMutationType;
import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

/**
 * This is a simple operator that formats the incoming message that is compatible with Kudu execution context
 */
public class KafkaStreamInputOperator extends AbstractKafkaInputOperator
{

  public final transient DefaultOutputPort<KuduExecutionContext<TransactionPayload>> outputForTransactionWrites =
    new DefaultOutputPort<>();


  public final transient DefaultOutputPort<KuduExecutionContext<TransactionPayload>> outputForDeviceWrites =
    new DefaultOutputPort<>();

  private Set<String> doNotWriteColumnsForTransactionsTable = new HashSet<>();

  private static final transient Logger LOG = LoggerFactory.getLogger(KafkaStreamInputOperator.class);

  ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    doNotWriteColumnsForTransactionsTable.add("transaction_amnt");
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

  private KuduExecutionContext<TransactionPayload> resolveExecutionContextForTransactTable(TransactionPayload payload)
  {
    KuduExecutionContext<TransactionPayload> context = new KuduExecutionContext<>();
    context.setPayload(payload);
    context.setMutationType(KuduMutationType.INSERT);
    return context;
  }


  private KuduExecutionContext<TransactionPayload> resolveExecutionContextForDevicesTable(TransactionPayload payload)
  {
    KuduExecutionContext<TransactionPayload> context = new KuduExecutionContext<>();
    context.setPayload(payload);
    context.setMutationType(KuduMutationType.UPSERT);
    context.setPropagatedTimestamp(payload.getTimestamp());
    return context;
  }


  private KuduExecutionContext<TransactionPayload> resolveExecutionContextForUpdate(TransactionPayload payload)
  {
    KuduExecutionContext<TransactionPayload> context = new KuduExecutionContext<>();
    context.setPayload(payload);
    context.setMutationType(KuduMutationType.UPDATE);
    context.setDoNotWriteColumns(doNotWriteColumnsForTransactionsTable);
    return context;
  }

}
