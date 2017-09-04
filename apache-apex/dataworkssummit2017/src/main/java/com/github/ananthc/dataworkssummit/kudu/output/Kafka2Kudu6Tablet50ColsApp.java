package com.github.ananthc.dataworkssummit.kudu.output;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

/**
 * Created by Ananth on 4/9/17.
 */
public class Kafka2Kudu6Tablet50ColsApp extends BaseKafkaToKuduOutputApplication<FiftyColsPojo>
{

  public void ensureTablePresent() throws Exception
  {
    ensureTablePresent("FiftyCols6Tablets", 5, 30, 8,10);
  }

  public void setKafkaTopic(KafkaStreamInputOperator kafkaStreamInputOperator) throws Exception
  {
    kafkaStreamInputOperator.setTopics("kudu6tablets50cols");
  }

  @Override
  protected KafkaStreamInputOperator getKafkaInputOperatorInstance()
  {
    return new KafkaStreamInputOperator<FiftyColsPojo>(FiftyColsPojo.class);
  }

  @Override
  protected String getPropertyFileName()
  {
    return "kuduoutput50cols6tablet.properties";
  }
}
