package com.github.ananthc.dataworkssummit.datagenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.lib.util.PojoUtils;

public class KafkaPojoDataGenerator

{
  private final KafkaProducer<Integer, String> producer;
  private final String topic;

  private static final int NUM_MESSAGES = 5000000;

  private static final Random random = new Random();

  private Map<Integer,FiftyColsPojo> lookupOfRecords = new HashMap<>();


  public KafkaPojoDataGenerator(String topic, String hosts)
  {
    Properties props = new Properties();
    props.put("bootstrap.servers", hosts);
    props.put("client.id", "KuduOutputProducer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producer = new KafkaProducer<>(props);
    this.topic = topic;
  }

  public void writeMessage(int messageKey, String message)
  {
    try {
      producer.send(new ProducerRecord<>(topic,
        messageKey,
        message)).get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }


  public void generateTestDataSetFor25Cols(int numRanges) throws IOException
  {
    Map<String,Object> settersMap = new HashMap<>();
    generateSetters(settersMap,FiftyColsPojo.class,12,3,8);
    int rangeBlockSize = Integer.MAX_VALUE / numRanges;
    int numRangeValue = 0;
    int[] numRangeBoundaries = new int[numRanges];
    int counterForNumRanges = 0;
    while ( (numRangeValue <= Integer.MAX_VALUE)  || (counterForNumRanges < numRanges)) {
      if ( counterForNumRanges < numRanges ) {
        numRangeBoundaries[counterForNumRanges] = numRangeValue;
        counterForNumRanges ++;
        numRangeValue += rangeBlockSize;
      }
    }
    List<String> messages = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (int i=0; i < NUM_MESSAGES; i++) {
      FiftyColsPojo aPayload = new FiftyColsPojo();
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

  public void generateTestDataSetFor50COls(int numRanges) throws IOException
  {
    Map<String,Object> settersMap = new HashMap<>();
    generateSetters(settersMap,FiftyColsPojo.class,30,8,10);
    int rangeBlockSize = Integer.MAX_VALUE / numRanges;
    int numRangeValue = 0;
    int[] numRangeBoundaries = new int[numRanges];
    int counterForNumRanges = 0;
    while ( (numRangeValue <= Integer.MAX_VALUE)  || (counterForNumRanges < numRanges)) {
      if ( counterForNumRanges < numRanges ) {
        numRangeBoundaries[counterForNumRanges] = numRangeValue;
        counterForNumRanges ++;
        numRangeValue += rangeBlockSize;
      }
    }
    List<String> messages = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (int i=0; i < NUM_MESSAGES; i++) {
      FiftyColsPojo aPayload = new FiftyColsPojo();
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
    settersCollection.put("introwkey", PojoUtils.createSetterInt(clazzHandle,"introwkey"));
    settersCollection.put("timestamprowkey", PojoUtils.createSetterInt(clazzHandle,"timestamprowkey"));
  }

  public static void main(String[] args)
  {
    KafkaPojoDataGenerator kafkaPojoDataGenerator = new KafkaPojoDataGenerator("fiftycolpojos3tablets",
        "192.168.1.204:9092,192.168.1.140:9092,192.168.1.209:9092");
    try {
      kafkaPojoDataGenerator.generateTestDataSetFor50COls(3);
    } catch (IOException e) {
      e.printStackTrace();
    }
    kafkaPojoDataGenerator = new KafkaPojoDataGenerator("fiftycolpojos6tablets",
      "192.168.1.204:9092,192.168.1.140:9092,192.168.1.209:9092");
    try {
      kafkaPojoDataGenerator.generateTestDataSetFor50COls(6);
    } catch (IOException e) {
      e.printStackTrace();
    }

    kafkaPojoDataGenerator = new KafkaPojoDataGenerator("fiftycolpojos12tablets",
      "192.168.1.204:9092,192.168.1.140:9092,192.168.1.209:9092");
    try {
      kafkaPojoDataGenerator.generateTestDataSetFor50COls(12);
    } catch (IOException e) {
      e.printStackTrace();
    }


    kafkaPojoDataGenerator = new KafkaPojoDataGenerator("twentyfivecolpojos3tablets",
      "192.168.1.204:9092,192.168.1.140:9092,192.168.1.209:9092");
    try {
      kafkaPojoDataGenerator.generateTestDataSetFor50COls(3);
    } catch (IOException e) {
      e.printStackTrace();
    }


  }

}
