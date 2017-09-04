package com.github.ananthc.dataworkssummit.kudu.output;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;
import com.github.ananthc.dataworkssummit.pojos.HundredColsPojo;
import com.github.ananthc.dataworkssummit.pojos.TwentyFiveColsPojo;

/**
 * Created by Ananth on 4/9/17.
 */
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
}
