package com.github.ananthc.dataworkssummit.kudu.input.operators;

import org.apache.apex.malhar.kudu.AbstractKuduInputOperator;
import org.apache.apex.malhar.kudu.ApexKuduConnection;
import org.apache.apex.malhar.kudu.InputOperatorControlTuple;
import org.apache.apex.malhar.kudu.scanner.KuduRecordWithMeta;

import com.github.ananthc.dataworkssummit.pojos.FiftyColsPojo;

/**
 * Created by Ananth on 17/9/17.
 */
public class Kudu3Tablets50ColInputOperator extends AbstractKuduInputOperator<FiftyColsPojo,InputOperatorControlTuple>
{
  int iterationCounter = 1;

  public Kudu3Tablets50ColInputOperator()
  {
  }

  public Kudu3Tablets50ColInputOperator(ApexKuduConnection.ApexKuduConnectionBuilder kuduConnectionInfo,
     Class<FiftyColsPojo> clazzForPOJO) throws Exception
  {
    super(kuduConnectionInfo, clazzForPOJO);
  }

  @Override
  protected String getNextQuery()
  {
    System.out.println("Executing next query");
    return "select * from FiftyCols3Tablets using options CONTROLTUPLE_MESSAGE = \"end of iteration " +
      iterationCounter++ + "\"";
  }

  @Override
  protected KuduRecordWithMeta<FiftyColsPojo> processNextTuple()
  {
    return super.processNextTuple();
  }
}
