package com.github.ananthc.dataworkssummit.datagenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ananthc.dataworkssummit.pojos.BasePojo;
import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;
import com.github.ananthc.dataworkssummit.pojos.HundredColsPojo;
import com.github.ananthc.dataworkssummit.pojos.TwentyFiveColsPojo;

import com.datatorrent.lib.util.PojoUtils;

public class KafkaPojoDataGenerator

{
  private final KafkaProducer<Integer, String> producer;
  private final String topic;

  private static final int NUM_MESSAGES = 1000000;

  private static final Random random = new Random();

  public KafkaPojoDataGenerator(String topic, String hosts)
  {
    Properties props = new Properties();
    props.put("bootstrap.servers", hosts);
    props.put("client.id", "KuduOutputProducer");
    props.put("retries", 10);
    props.put("max.in.flight.requests.per.connection",1);
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<>(props);
    this.topic = topic;
  }

  public void writeMessage(int messageKey, String message)
  {
    //System.out.println(message);
    try {
      producer.send(new ProducerRecord<>(topic,
        messageKey,
        message)).get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void generateTestDataSet(int numRanges, Class clazzHandle, int numIntCols, int floatCols,
    int strCols) throws IOException, InterruptedException, IllegalAccessException, InstantiationException
  {
    Map<String,Object> settersMap = new HashMap<>();
    generateSetters(settersMap,clazzHandle,numIntCols,floatCols,strCols);
    int rangeBlockSize = Integer.MAX_VALUE / numRanges;
    int numRangeValue = 0;
    int[] numRangeBoundaries = new int[numRanges];
    int counterForNumRanges = 0;
    while ( (numRangeValue <= Integer.MAX_VALUE)  && (counterForNumRanges < numRanges)) {
      if ( counterForNumRanges < numRanges ) {
        numRangeBoundaries[counterForNumRanges] = numRangeValue;
        counterForNumRanges ++;
        numRangeValue += rangeBlockSize;
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    for (int i=0; i < NUM_MESSAGES; i++) {
      BasePojo aPayload = null;
      if (clazzHandle.getName().endsWith("FiftyColsPojo")) {
        aPayload = new FiftyColsPojo();
      }
      if (clazzHandle.getName().endsWith("TwentyFiveColsPojo")) {
        aPayload = new TwentyFiveColsPojo();
      }
      if (clazzHandle.getName().endsWith("HundredColsPojo")) {
        aPayload = new HundredColsPojo();
      }
      if ( aPayload == null) {
        throw new InstantiationException("Class instantiation is not supported inside data generator");
      }
      for(String colName : settersMap.keySet()) {
        if ( colName.startsWith("int")) {
          ((PojoUtils.SetterInt)settersMap.get(colName)).set(aPayload,random.nextInt());
        }
        if ( colName.startsWith("float")) {
          ((PojoUtils.SetterFloat)settersMap.get(colName)).set(aPayload,random.nextInt());
        }
        if ( colName.startsWith("str")) {
          ((PojoUtils.Setter)settersMap.get(colName)).set(aPayload,""+System.currentTimeMillis());
        }
      }
      aPayload.setTimestampRowKey(System.currentTimeMillis());
      aPayload.setIntRowKey(numRangeBoundaries[ i % numRangeBoundaries.length ] + i);
      writeMessage(i, mapper.writeValueAsString(aPayload));
    }
  }

  private void generateSetters(Map<String,Object> settersCollection, Class clazzHandle, int numIntCols, int floatCols,
      int strCols)
  {
    for ( int i =0; i < numIntCols ; i++) {
      settersCollection.put("int"+i,PojoUtils.createSetterInt(clazzHandle,"int"+i));
    }
    for ( int i =0; i < floatCols ; i++) {
      settersCollection.put("float"+i,PojoUtils.createSetterFloat(clazzHandle,"float"+i));
    }
    for ( int i =0; i < strCols ; i++) {
      settersCollection.put("str"+i,PojoUtils.createSetter(clazzHandle,"str"+i, String.class));
    }
    settersCollection.put("intRowKey", PojoUtils.createSetterInt(clazzHandle,"intRowKey"));
    settersCollection.put("timestampRowKey", PojoUtils.createSetterInt(clazzHandle,"timestampRowKey"));
  }

  public static void main(String[] args) throws InterruptedException, InstantiationException, IllegalAccessException
  {
    KafkaPojoDataGenerator kafkaPojoDataGenerator = new KafkaPojoDataGenerator("kudu3tablets50cols",
      "192.168.1.39:9092,192.168.1.230:9092,192.168.1.209:9092");
    try {
      kafkaPojoDataGenerator.generateTestDataSet(3, FiftyColsPojo.class, 30,8,10);
    } catch (IOException e) {
      e.printStackTrace();
    }
//    kafkaPojoDataGenerator = new KafkaPojoDataGenerator("kudu6tablets50cols",
//      "192.168.1.39:9092,192.168.1.230:9092,192.168.1.209:9092");
//    try {
//      kafkaPojoDataGenerator.generateTestDataSet(6,FiftyColsPojo.class, 30,8,10);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    kafkaPojoDataGenerator = new KafkaPojoDataGenerator("kudu12tablets50cols",
//      "192.168.1.39:9092,192.168.1.230:9092,192.168.1.209:9092");
//    try {
//      kafkaPojoDataGenerator.generateTestDataSet(12,FiftyColsPojo.class, 30,8,10);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }


//    KafkaPojoDataGenerator  kafkaPojoDataGenerator = new KafkaPojoDataGenerator("kudu3tablets25cols",
//      "192.168.1.39:9092,192.168.1.230:9092,192.168.1.209:9092");
//    try {
//      kafkaPojoDataGenerator.generateTestDataSet(3,TwentyFiveColsPojo.class, 12,3,8);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//
//    kafkaPojoDataGenerator = new KafkaPojoDataGenerator("kudu3tablets100cols",
//      "192.168.1.39:9092,192.168.1.230:9092,192.168.1.209:9092");
//    try {
//      kafkaPojoDataGenerator.generateTestDataSet(3,HundredColsPojo.class, 60,18,20);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }

}
