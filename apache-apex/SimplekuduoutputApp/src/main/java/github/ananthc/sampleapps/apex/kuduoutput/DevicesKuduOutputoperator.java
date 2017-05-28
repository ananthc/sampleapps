package github.ananthc.sampleapps.apex.kuduoutput;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.kudu.BaseKuduOutputOperator;
import org.apache.apex.malhar.contrib.kudu.KuduExecutionContext;
import org.apache.apex.malhar.lib.wal.WindowDataManager;

/**
 * Created by Ananth on 28/5/17. NOt used in the application code and is for debugging purposes only
 */
public class DevicesKuduOutputoperator extends BaseKuduOutputOperator
{

  private static final transient Logger LOG = LoggerFactory.getLogger(DevicesKuduOutputoperator.class);


  public DevicesKuduOutputoperator() throws IOException, ClassNotFoundException
  {
  }

  public DevicesKuduOutputoperator(WindowDataManager windowManager, String configFileInClasspath)
    throws IOException, ClassNotFoundException
  {
    super(windowManager, configFileInClasspath);
  }

  public DevicesKuduOutputoperator(WindowDataManager windowManager) throws IOException, ClassNotFoundException
  {
    super(windowManager);
  }

  public DevicesKuduOutputoperator(String configFileInClasspath) throws IOException, ClassNotFoundException
  {
    super(configFileInClasspath);
  }

  public DevicesKuduOutputoperator(String kuduTableName, List<String> kuduMasters, Class pojoPayloadClass)
  {
    super(kuduTableName, kuduMasters, pojoPayloadClass);
  }

  @Override
  public void processTuple(KuduExecutionContext kuduExecutionContext)
  {
    LOG.info("processing for payload " + kuduExecutionContext.getPayload().toString());
    super.processTuple(kuduExecutionContext);
  }
}
