package com.github.ananthc.dataworkssummit.kudu.output;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="KafkaToKudu3TabletFiftyColsSyncApp")
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
}
