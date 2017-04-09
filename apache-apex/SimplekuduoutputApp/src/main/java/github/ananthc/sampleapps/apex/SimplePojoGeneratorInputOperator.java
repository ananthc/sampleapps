package github.ananthc.sampleapps.apex;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.contrib.kudu.KuduExecutionContext;
import org.apache.kudu.ColumnSchema;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class SimplePojoGeneratorInputOperator extends BaseOperator implements InputOperator
{

  public final transient DefaultOutputPort<KuduExecutionContext<SamplePojoPayload>> out = new DefaultOutputPort<>();

  @Override
  public void emitTuples()
  {

  }

  private SamplePojoPayload getPojoForInsert()
  {
    SamplePojoPayload
  }


  private void createAppTestTable(String tableName, KuduClient client) throws Exception
  {
    List<ColumnSchema> columns = new ArrayList<>();
    ColumnSchema intRowKeyCol = new ColumnSchema.ColumnSchemaBuilder("introwkey", Type.INT32)
      .key(true)
      .build();
    columns.add(intRowKeyCol);
    columnDefs.put("introwkey",intRowKeyCol);
    ColumnSchema stringRowKeyCol = new ColumnSchema.ColumnSchemaBuilder("stringrowkey", Type.STRING)
      .key(true)
      .build();
    columns.add(stringRowKeyCol);
    columnDefs.put("stringrowkey",stringRowKeyCol);
    ColumnSchema timestampRowKey = new ColumnSchema.ColumnSchemaBuilder("timestamprowkey", Type.UNIXTIME_MICROS)
      .key(true)
      .build();
    columns.add(timestampRowKey);
    columnDefs.put("timestamprowkey",timestampRowKey);
    ColumnSchema longData = new ColumnSchema.ColumnSchemaBuilder("longdata", Type.INT64)
      .build();
    columns.add(longData);
    columnDefs.put("longdata",longData);
    ColumnSchema stringData = new ColumnSchema.ColumnSchemaBuilder("stringdata", Type.STRING)
      .build();
    columns.add(stringData);
    columnDefs.put("stringdata",stringData);
    ColumnSchema timestampdata = new ColumnSchema.ColumnSchemaBuilder("timestampdata", Type.UNIXTIME_MICROS)
      .build();
    columns.add(timestampdata);
    columnDefs.put("timestampdata",timestampdata);
    ColumnSchema binarydata = new ColumnSchema.ColumnSchemaBuilder("binarydata", Type.BINARY)
      .build();
    columns.add(binarydata);
    columnDefs.put("binarydata",binarydata);
    ColumnSchema floatdata = new ColumnSchema.ColumnSchemaBuilder("floatdata", Type.FLOAT)
      .build();
    columns.add(floatdata);
    columnDefs.put("floatdata",floatdata);
    ColumnSchema booldata = new ColumnSchema.ColumnSchemaBuilder("booldata", Type.BOOL)
      .build();
    columns.add(booldata);
    columnDefs.put("booldata",booldata);
    List<String> rangeKeys = new ArrayList<>();
    rangeKeys.add("stringrowkey");
    rangeKeys.add("timestamprowkey");
    List<String> hashPartitions = new ArrayList<>();
    hashPartitions.add("introwkey");
    Schema schema = new Schema(columns);
    try {
      client.createTable(tableName, schema,
        new CreateTableOptions()
          .setNumReplicas(1)
          .setRangePartitionColumns(rangeKeys)
          .addHashPartitions(hashPartitions,2));
    } catch (KuduException e) {
      LOG.error("Error while creating table for unit tests " + e.getMessage(), e);
      throw e;
    }
  }

  private void lookUpAndPopulateRecord(UnitTestTablePojo keyInfo) throws Exception
  {
    KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable)
      .addPredicate(KuduPredicate.newComparisonPredicate(columnDefs.get("introwkey"),
        KuduPredicate.ComparisonOp.EQUAL,keyInfo.getIntrowkey()))
      .addPredicate(KuduPredicate.newComparisonPredicate(columnDefs.get("stringrowkey"),
        KuduPredicate.ComparisonOp.EQUAL,keyInfo.getStringrowkey()))
      .addPredicate(KuduPredicate.newComparisonPredicate(columnDefs.get("timestamprowkey"),
        KuduPredicate.ComparisonOp.EQUAL,keyInfo.getTimestamprowkey()))
      .build();
    RowResultIterator rowResultItr = scanner.nextRows();
    while (rowResultItr.hasNext()) {
      RowResult thisRow = rowResultItr.next();
      keyInfo.setFloatdata(thisRow.getFloat("floatdata"));
      keyInfo.setBooldata(thisRow.getBoolean("booldata"));
      keyInfo.setBinarydata(thisRow.getBinary("binarydata"));
      keyInfo.setLongdata(thisRow.getLong("longdata"));
      keyInfo.setTimestampdata(thisRow.getLong("timestampdata"));
      keyInfo.setStringdata("stringdata");
      break;
    }
  }
}
