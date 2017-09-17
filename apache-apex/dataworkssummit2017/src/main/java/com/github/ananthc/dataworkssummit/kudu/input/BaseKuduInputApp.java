package com.github.ananthc.dataworkssummit.kudu.input;

import java.util.Properties;

import javax.validation.ConstraintViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.ApexKuduConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.kudu.client.KuduClient;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;

/**
 * Created by Ananth on 17/9/17.
 */
public abstract class BaseKuduInputApp implements StreamingApplication
{

  private static final transient Logger LOG = LoggerFactory.getLogger(BaseKuduInputApp.class);

  public abstract String getTableName();

  private KuduClient getClientHandle() throws Exception
  {
    KuduClient.KuduClientBuilder builder = new KuduClient.KuduClientBuilder("192.168.1.41:7051");
    KuduClient client = builder.build();
    return client;
  }

  public ApexKuduConnection.ApexKuduConnectionBuilder getApexConnectionBuilder()
  {
    ApexKuduConnection.ApexKuduConnectionBuilder apexKuduConnectionBuilder =
       new ApexKuduConnection.ApexKuduConnectionBuilder();
    return apexKuduConnectionBuilder.withAPossibleMasterHostAs("192.168.1.41:7051").withTableName(getTableName());
  }


}
