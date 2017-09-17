package com.github.ananthc.dataworkssummit.kudu.output;

import javax.validation.ConstraintViolationException;

import org.apache.hadoop.conf.Configuration;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;
import com.github.ananthc.dataworkssummit.pojos.HundredColsPojo;
import com.github.ananthc.dataworkssummit.pojos.TwentyFiveColsPojo;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Created by Ananth on 4/9/17.
 */
@ApplicationAnnotation(name="Kafka2Kudu3Tablet100ColsApp")
public class Kafka2Kudu3Tablet100ColsApp  extends BaseKafkaToKuduOutputApplication<HundredColsPojo>
{

  public void ensureTablePresent() throws Exception
  {
    ensureTablePresent("HundredCols3Tablets", 2, 60, 18,20);
  }

  public void setKafkaTopic(KafkaStreamInputOperator kafkaStreamInputOperator) throws Exception
  {
    kafkaStreamInputOperator.setTopics("kudu3tablets100cols");
  }

  @Override
  protected KafkaStreamInputOperator getKafkaInputOperatorInstance()
  {
    return new KafkaStreamInputOperator<FiftyColsPojo>(HundredColsPojo.class);
  }

  @Override
  protected String getPropertyFileName()
  {
    return "kuduoutput100cols3tablet.properties";
  }

  public static void main(String[] args)
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kafka2Kudu3Tablet100ColsApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
