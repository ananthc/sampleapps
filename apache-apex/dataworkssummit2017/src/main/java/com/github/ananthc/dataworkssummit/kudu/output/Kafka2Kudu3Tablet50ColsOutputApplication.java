package com.github.ananthc.dataworkssummit.kudu.output;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="Kafka2Kudu3Tablet50ColsOutputApplication")
public class Kafka2Kudu3Tablet50ColsOutputApplication extends BaseKafkaToKuduOutputApplication<FiftyColsPojo>
{

  public void ensureTablePresent() throws Exception
  {
    ensureTablePresent("FiftyCols3Tablets", 2, 30, 8,10);
  }

  public void setKafkaTopic(KafkaStreamInputOperator kafkaStreamInputOperator) throws Exception
  {
    kafkaStreamInputOperator.setTopics("kudu3tablets50cols");
  }

  @Override
  protected KafkaStreamInputOperator getKafkaInputOperatorInstance()
  {
    return new KafkaStreamInputOperator<FiftyColsPojo>(FiftyColsPojo.class);
  }

  @Override
  protected String getPropertyFileName()
  {
    return "kuduoutput50cols3tablet.properties";
  }

  public static void main(String[] args)
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kafka2Kudu3Tablet50ColsOutputApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(45000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
