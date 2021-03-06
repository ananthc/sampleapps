/**
 * Put your copyright and license info here.
 */
package github.ananthc.sampleapps.apex.kuduoutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kudu.BaseKuduOutputOperator;
import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.PartitionStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="KafkaToKuduSyncApp")
public class KafkaToKuduOutputApplication implements StreamingApplication
{

  private static final transient Logger LOG = LoggerFactory.getLogger(KafkaToKuduOutputApplication.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ensureTablesPresent("transactions","zetadevices");
    KafkaStreamInputOperator kafkaInput = new KafkaStreamInputOperator();
    Properties props = new Properties();
    props.put("client.id","KuduoutputApexApp-"+System.currentTimeMillis());
    kafkaInput.setClusters("192.168.1.204:9092");
    kafkaInput.setTopics("transactionfeeds");
    kafkaInput.setConsumerProps(props);
    kafkaInput.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    kafkaInput.setStrategy(PartitionStrategy.ONE_TO_MANY.name());
    BaseKuduOutputOperator deviceStatusTableKuduOutputOperator = null;
    TransactionsTableKuduOutputOperator transactionsTableKuduOutputOperator = null;
    try {
      deviceStatusTableKuduOutputOperator = new BaseKuduOutputOperator();
      transactionsTableKuduOutputOperator = new TransactionsTableKuduOutputOperator("transactiontable.properties");
    } catch (IOException| ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    dag.addOperator("kafkaInput",kafkaInput);
    dag.addOperator("devicestatuskuduoutput",deviceStatusTableKuduOutputOperator);
    dag.addOperator("transactionstatuskuduoutput",transactionsTableKuduOutputOperator);
    dag.addStream("kafka2devicestatus",kafkaInput.outputForDeviceWrites, deviceStatusTableKuduOutputOperator.input);
    dag.addStream("kafka2transactions",kafkaInput.outputForTransactionWrites, transactionsTableKuduOutputOperator.input);
  }

  private KuduClient getClientHandle() throws Exception
  {
    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder("192.168.1.41:7051");
    KuduClient client = builder.build();
    return client;
  }

  public void ensureTablesPresent(String transactionstableName,String devicesTableName)
  {
    try {
      KuduClient kuduClient = getClientHandle();
      if (kuduClient.tableExists(transactionstableName)) {
        kuduClient.deleteTable(transactionstableName);
      }
      if (kuduClient.tableExists(devicesTableName)) {
        kuduClient.deleteTable(devicesTableName);
      }
      createTableForTransactions(transactionstableName, kuduClient);
      createTableForDevices(devicesTableName,kuduClient);
      kuduClient.shutdown();

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createTableForDevices(String tableName, KuduClient client) throws Exception
  {

    List<ColumnSchema> columnsForDevicesTable = new ArrayList<>();
    ColumnSchema deviceIdCol = new ColumnSchema.ColumnSchemaBuilder("deviceid", Type.STRING)
      .key(true)
      .build();
    columnsForDevicesTable.add(deviceIdCol);
    ColumnSchema timestampCol = new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS)
      .key(true)
      .build();
    columnsForDevicesTable.add(timestampCol);
    ColumnSchema transactionAmtCol = new ColumnSchema.ColumnSchemaBuilder("transactionAmount", Type.DOUBLE)
      .key(false)
      .build();
    columnsForDevicesTable.add(transactionAmtCol);


    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("timestamp");
    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("deviceid");


    Schema schemaForDevicesTable = new Schema(columnsForDevicesTable);
    try {
      client.createTable(tableName, schemaForDevicesTable,
        new CreateTableOptions()
          .setNumReplicas(3)
          .setRangePartitionColumns(rangeKeys)
          .addHashPartitions(hashPartitions,7));
    } catch (KuduException e) {
      LOG.error("Error while creating table for unit tests " + e.getMessage(), e);
      throw e;
    }

  }

    private void createTableForTransactions(String tableName, KuduClient client) throws Exception
  {
    List<ColumnSchema> columnsForTransactionsTable = new ArrayList<>();
    ColumnSchema transactionidCol = new ColumnSchema.ColumnSchemaBuilder("transactionid", Type.STRING)
      .key(true)
      .build();
    columnsForTransactionsTable.add(transactionidCol);
    ColumnSchema timestampCol = new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS)
      .key(true)
      .build();
    columnsForTransactionsTable.add(timestampCol);
    ColumnSchema isStepUpTransactionCol = new ColumnSchema.ColumnSchemaBuilder("is_stepup", Type.BOOL)
      .key(false)
      .build();
    columnsForTransactionsTable.add(isStepUpTransactionCol);
    ColumnSchema transactionAmtCol = new ColumnSchema.ColumnSchemaBuilder("transaction_amnt", Type.DOUBLE)
      .key(false)
      .build();
    columnsForTransactionsTable.add(transactionAmtCol);


    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("timestamp");
    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("transactionid");

    Schema schemaForTransactionsTable = new Schema(columnsForTransactionsTable);
    try {
      client.createTable(tableName, schemaForTransactionsTable,
        new CreateTableOptions()
          .setNumReplicas(3)
          .setRangePartitionColumns(rangeKeys)
          .addHashPartitions(hashPartitions,5));
    } catch (KuduException e) {
      LOG.error("Error while creating table for unit tests " + e.getMessage(), e);
      throw e;
    }
  }
}
