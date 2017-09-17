package com.github.ananthc.dataworkssummit.kudu.output;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Created by Ananth on 4/9/17.
 */
@ApplicationAnnotation(name="Kafka2Kudu12Tablet50ColsApp")
public class Kafka2Kudu12Tablet50ColsApp  extends BaseKafkaToKuduOutputApplication<FiftyColsPojo>
{

  public void ensureTablePresent() throws Exception
  {
    ensureTablePresent("FiftyCols12Tablets", 11, 30, 8,10);
  }

  public void setKafkaTopic(KafkaStreamInputOperator kafkaStreamInputOperator) throws Exception
  {
    kafkaStreamInputOperator.setTopics("kudu12tablets50cols");
  }

  @Override
  protected KafkaStreamInputOperator getKafkaInputOperatorInstance()
  {
    return new KafkaStreamInputOperator<FiftyColsPojo>(FiftyColsPojo.class);
  }

  @Override
  protected String getPropertyFileName()
  {
    return "kuduoutput50cols12tablet.properties";
  }

  public static void main(String[] args)
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kafka2Kudu12Tablet50ColsApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
