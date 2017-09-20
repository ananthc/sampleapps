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
import org.apache.kudu.client.PartialRow;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.netlet.util.DTThrowable;

public abstract class BaseKafkaToKuduOutputApplication<T> implements StreamingApplication
{

  private static final transient Logger LOG = LoggerFactory.getLogger(BaseKafkaToKuduOutputApplication.class);

  public abstract void ensureTablePresent()  throws Exception;

  public abstract void setKafkaTopic(KafkaStreamInputOperator kafkaInput)  throws Exception;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    try {
      ensureTablePresent();
      KafkaStreamInputOperator<T> kafkaInput = getKafkaInputOperatorInstance();
      Properties props = new Properties();
      //props.put("client.id","KuduoutputApexApp-"+System.currentTimeMillis());
      kafkaInput.setClusters("192.168.1.39:9092");
      setKafkaTopic(kafkaInput);
      kafkaInput.setConsumerProps(props);
      kafkaInput.setInitialOffset(AbstractKafkaInputOperator.InitialOffset.EARLIEST.name());
      kafkaInput.setStrategy(PartitionStrategy.ONE_TO_ONE.name());
      BaseKuduOutputOperator tableKuduOutputOperator = null;
      try {
        tableKuduOutputOperator = new BaseKuduOutputOperator(getPropertyFileName());
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      dag.addOperator("kafkaInput",kafkaInput);
      dag.addOperator("tableOutput",tableKuduOutputOperator);
      dag.addStream("kafka2tableoutput",kafkaInput.outputForWrites, tableKuduOutputOperator.input);
    } catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }

  }

  protected abstract KafkaStreamInputOperator<T> getKafkaInputOperatorInstance();

  protected abstract String getPropertyFileName();

  private KuduClient getClientHandle() throws Exception
  {
    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder("192.168.1.41:7051");
    KuduClient client = builder.build();
    return client;
  }

  public void ensureTablePresent(String tableName,int numRanges, int numIntCols, int numFloatCols,
    int numStrCols) throws Exception
  {
    try {
      KuduClient kuduClient = getClientHandle();
      if (kuduClient.tableExists(tableName)) {
        kuduClient.deleteTable(tableName);
      }
      createTable(tableName,kuduClient,numRanges,numIntCols,numFloatCols,numStrCols);
      kuduClient.shutdown();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void createTable(String tableName, KuduClient client, int numRanges, int numIntCols, int numFloatCols,
      int numStrCols) throws Exception
  {

    List<ColumnSchema> columnsForTable = new ArrayList<>();
    ColumnSchema intRowKeyCol = new ColumnSchema.ColumnSchemaBuilder("intRowKey", Type.INT32)
      .key(true)
      .build();
    columnsForTable.add(intRowKeyCol);
    ColumnSchema timestampCol = new ColumnSchema.ColumnSchemaBuilder("timestampRowKey", Type.INT64)
      .key(true)
      .build();
    columnsForTable.add(timestampCol);
    /*
    ColumnSchema intKeyCol = new ColumnSchema.ColumnSchemaBuilder("int0", Type.INT32)
      .key(true)
      .build();
    columnsForTable.add(intKeyCol);
    */
    for ( int i=0; i < numIntCols; i++) {
      ColumnSchema intCol = new ColumnSchema.ColumnSchemaBuilder(("int"+i), Type.INT32)
        .key(false)
        .nullable(true)
        .build();
      columnsForTable.add(intCol);
    }
    for ( int i=0; i < numFloatCols; i++) {
      ColumnSchema floatCol = new ColumnSchema.ColumnSchemaBuilder(("float"+i), Type.FLOAT)
        .key(false)
        .nullable(true)
        .build();
      columnsForTable.add(floatCol);
    }
    for ( int i=0; i < numStrCols; i++) {
      ColumnSchema strCol = new ColumnSchema.ColumnSchemaBuilder(("str"+i), Type.STRING)
        .key(false)
        .nullable(true)
        .build();
      columnsForTable.add(strCol);
    }
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("intRowKey");
    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("timestampRowKey");
    //hashPartitions.add("int0");
    Schema schemaForTable = new Schema(columnsForTable);
    int stepsize = Integer.MAX_VALUE / numRanges;
    int splitBoundary = stepsize;
    CreateTableOptions createTableOptions = new CreateTableOptions()
      .setNumReplicas(3)
      .setRangePartitionColumns(rangeKeys)
      .addHashPartitions(hashPartitions,2);
    for ( int i = 0; i < numRanges; i++) {
      PartialRow splitRowBoundary = schemaForTable.newPartialRow();
      splitRowBoundary.addInt("intRowKey",splitBoundary);
      createTableOptions = createTableOptions.addSplitRow(splitRowBoundary);
      splitBoundary += stepsize;
    }
    try {
      client.createTable(tableName, schemaForTable,createTableOptions);
    } catch (KuduException e) {
      LOG.error("Error while creating table for unit tests " + e.getMessage(), e);
      throw e;
    }
  }

}
