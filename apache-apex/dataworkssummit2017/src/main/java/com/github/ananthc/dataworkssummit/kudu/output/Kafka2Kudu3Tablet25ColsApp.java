package com.github.ananthc.dataworkssummit.kudu.output;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;
import com.github.ananthc.dataworkssummit.pojos.TwentyFiveColsPojo;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Created by Ananth on 4/9/17.
 */
@ApplicationAnnotation(name="Kafka2Kudu3Tablet25ColsApp")
public class Kafka2Kudu3Tablet25ColsApp extends BaseKafkaToKuduOutputApplication<TwentyFiveColsPojo>
{

  public void ensureTablePresent() throws Exception
  {
    ensureTablePresent("TwentyFiveCols3Tablets", 2, 12, 3,8);
  }

  public void setKafkaTopic(KafkaStreamInputOperator kafkaStreamInputOperator) throws Exception
  {
    kafkaStreamInputOperator.setTopics("kudu3tablets25cols");
  }

  @Override
  protected KafkaStreamInputOperator getKafkaInputOperatorInstance()
  {
    return new KafkaStreamInputOperator<FiftyColsPojo>(TwentyFiveColsPojo.class);
  }

  @Override
  protected String getPropertyFileName()
  {
    return "kuduoutput25cols3tablet.properties";
  }

  public static void main(String[] args)
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kafka2Kudu3Tablet25ColsApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

