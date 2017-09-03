/**
 * Put your copyright and license info here.
 */
package com.github.ananthc.dataworkssummit.kudu.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.apex.malhar.kafka.PartitionStrategy;
import org.apache.apex.malhar.kudu.BaseKuduOutputOperator;
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
    ensureTablesPresent();
    KafkaStreamInputOperator kafkaInput = new KafkaStreamInputOperator();
    Properties props = new Properties();
    props.put("client.id","KuduoutputApexApp-"+System.currentTimeMillis());
    kafkaInput.setClusters();
    kafkaInput.setTopics();
    kafkaInput.setConsumerProps(props);
    kafkaInput.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
    kafkaInput.setStrategy(PartitionStrategy.ONE_TO_MANY.name());
    BaseKuduOutputOperator tableKuduOutputOperator = null;
    try {
      tableKuduOutputOperator = new BaseKuduOutputOperator(asa);
    } catch (IOException| ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    dag.addOperator("kafkaInput",kafkaInput);
    dag.addOperator("tableOutput",tableKuduOutputOperator);
    dag.addStream("kafka2tableoutput",kafkaInput.outputFor25ColTransactionWrites, tableKuduOutputOperator.input);

  }

  private KuduClient getClientHandle() throws Exception
  {
    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder();
    KuduClient client = builder.build();
    return client;
  }

  public void ensureTablesPresent(String tableName)
  {
    try {
      KuduClient kuduClient = getClientHandle();
      if (kuduClient.tableExists(tableName)) {
        kuduClient.deleteTable(tableName);
      }
      createTable(tableName,kuduClient);
      kuduClient.shutdown();

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createTable(String tableName, KuduClient client) throws Exception
  {

    List<ColumnSchema> columnsForTable = new ArrayList<>();
    ColumnSchema deviceIdCol = new ColumnSchema.ColumnSchemaBuilder("introwkey", Type.INT32)
      .key(true)
      .build();
    columnsForTable.add(deviceIdCol);
    ColumnSchema timestampCol = new ColumnSchema.ColumnSchemaBuilder("timestamprowkey", Type.INT64)
      .key(true)
      .build();
    columnsForTable.add(timestampCol);


    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("introwkey");

    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("timestamprowkey");


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

}
