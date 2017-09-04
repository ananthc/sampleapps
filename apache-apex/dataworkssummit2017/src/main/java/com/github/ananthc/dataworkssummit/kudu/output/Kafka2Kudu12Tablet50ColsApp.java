package com.github.ananthc.dataworkssummit.kudu.output;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

/**
 * Created by Ananth on 4/9/17.
 */
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
}
