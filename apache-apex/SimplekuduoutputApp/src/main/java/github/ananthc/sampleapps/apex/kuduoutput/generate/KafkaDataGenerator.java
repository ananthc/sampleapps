package github.ananthc.sampleapps.apex.kuduoutput.generate;

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

import github.ananthc.sampleapps.apex.kuduoutput.TransactionPayload;

public class KafkaDataGenerator

{
  private final KafkaProducer<Integer, String> producer;
  private final String topic;

  private static final int NUM_MESSAGES = 1000000;
  private static final  int STEPUP_EVERY_NTH_TUPLE = 100;

  private static final int OUT_OF_BAND__TRANSACTION_EVERY_NTH_TUPLE = 150;

  private static final Random random = new Random();

  private Map<Integer,TransactionPayload> lookupOfRecords = new HashMap<>();


  public KafkaDataGenerator(String topic, String hosts)
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

  public void writeTestDataSet() throws IOException
  {
    List<String> allMsgs = generateTestDataSet();
    int i=0;
    for(String aMessage : allMsgs) {
      writeMessage(i,aMessage);
      i++;
    }
  }

  public List<String> generateTestDataSet() throws IOException
  {
    List<String> messages = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();

    for (int i=0; i < NUM_MESSAGES; i++) {
      TransactionPayload aPayload = new TransactionPayload();
      aPayload.setDeviceId(""+i);
      aPayload.setTransactionId(""+i);
      aPayload.setTimestamp(System.currentTimeMillis() * 1000);
      aPayload.setTransactionAmount(random.nextDouble() * 1000);
      aPayload.setTransactionEditMode('i');
      aPayload.setStepUp(false);
      lookupOfRecords.put(i,aPayload);
      messages.add(mapper.writeValueAsString(aPayload));
      if (i% OUT_OF_BAND__TRANSACTION_EVERY_NTH_TUPLE == 0) {
        // generate an out of bnd data after every NTH window
        aPayload.setTimestamp(aPayload.getTimestamp()-100000000L); // delay by 100000 seconds but arrive after
        messages.add(mapper.writeValueAsString(aPayload));
      }
    }
    for (int i=0; i < NUM_MESSAGES; i++) {
      if (i% STEPUP_EVERY_NTH_TUPLE == 0) {
        TransactionPayload aPayload = lookupOfRecords.get(i);
        aPayload.setTransactionEditMode('u');
        aPayload.setStepUp(true);
        aPayload.setTransactionAmount(null); // Simulate transaction amount not coming through
        messages.add(mapper.writeValueAsString(aPayload));
      }
    }
    return messages;
  }

  public static void main(String[] args)
  {
    KafkaDataGenerator kafkaDataGenerator = new KafkaDataGenerator("transactionfeeds", "192.168.1.204:9092,192.168.1.140:9092,192.168.1.209:9092");
    try {
      kafkaDataGenerator.writeTestDataSet();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
