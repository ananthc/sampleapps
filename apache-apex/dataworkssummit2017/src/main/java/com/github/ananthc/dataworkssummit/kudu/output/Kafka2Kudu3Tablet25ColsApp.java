package com.github.ananthc.dataworkssummit.kudu.output;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;
import com.github.ananthc.dataworkssummit.pojos.TwentyFiveColsPojo;

/**
 * Created by Ananth on 4/9/17.
 */
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
}
