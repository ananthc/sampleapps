package com.github.ananthc.dataworkssummit.kudu.input;

import java.util.Properties;

import javax.validation.ConstraintViolationException;

import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.github.ananthc.dataworkssummit.kudu.input.operators.Kudu3Tablets50ColInputOperator;
import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 * Created by Ananth on 17/9/17.
 */
public class Kudu3Tablets50ColsInputApp extends BaseKuduInputApp
{
  @Override
  public String getTableName()
  {
    return "FiftyCols3Tablets";
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    try {
      KafkaSinglePortOutputOperator<String,FiftyColsPojo> kafkaOutput = getKafkaOutputOperator();
      Kudu3Tablets50ColInputOperator kudu3Tablets50ColInputOperator = new Kudu3Tablets50ColInputOperator(
        getApexConnectionBuilder(), FiftyColsPojo.class);
      dag.addOperator("kuduInput",kudu3Tablets50ColInputOperator);
      dag.addOperator("kafkaOutput",kafkaOutput);
      dag.addStream("kudu2kafkaoutput",kudu3Tablets50ColInputOperator.outputPort, kafkaOutput.inputPort);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public KafkaSinglePortOutputOperator<String,FiftyColsPojo> getKafkaOutputOperator()
  {
    KafkaSinglePortOutputOperator<String,FiftyColsPojo> kafkaOutput = new KafkaSinglePortOutputOperator<>();
    kafkaOutput.setTopic("allcolumnDump");
    Properties props = new Properties();
    props.put("serializer.class","kafka.serializer.StringEncoder");
    props.put("metadata.broker.list","192.168.1.39:9092");
    props.put("producer.type","async");
    kafkaOutput.setProperties(props);
    return kafkaOutput;
  }

  public static void main(String[] args)
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kudu3Tablets50ColsInputApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
