package github.ananthc.sampleapps.apex.kuduoutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.contrib.kudu.ApexKuduConnection;
import org.apache.apex.malhar.contrib.kudu.BaseKuduOutputOperator;
import org.apache.apex.malhar.contrib.kudu.KuduExecutionContext;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by Ananth on 14/5/17.
 */
public class TransactionsTableKuduOutputOperator extends BaseKuduOutputOperator
{
  private ApexKuduConnection apexKuduConnectionForReads = null;

  private KuduSession sessionForReads = null;

  private KuduTable tableForReads = null;

  private ColumnSchema transactionidCol = null;

  private ColumnSchema timestampCol = null;

  private static final transient Logger LOG = LoggerFactory.getLogger(TransactionsTableKuduOutputOperator.class);


  public TransactionsTableKuduOutputOperator() throws IOException, ClassNotFoundException
  {
  }

  public TransactionsTableKuduOutputOperator(String configFileInClasspath) throws IOException, ClassNotFoundException
  {
    super(configFileInClasspath);
  }

  public TransactionsTableKuduOutputOperator(String kuduTableName, List<String> kuduMasters, Class pojoPayloadClass)
  {
    super(kuduTableName, kuduMasters, pojoPayloadClass);
  }

  @Override
  protected Map<String, String> getOverridingColumnNameMap()
  {
    Map<String,String> overridingColumns = new HashMap<>();
    overridingColumns.put("transactionAmount","transaction_amnt");
    return overridingColumns;
  }

  public void initConnectionForReads()
  {
    ApexKuduConnection.ApexKuduConnectionBuilder kuduClientConfig = this.getApexKuduConnectionBuilder();
    apexKuduConnectionForReads = kuduClientConfig.build();
    checkNotNull(apexKuduConnectionForReads,"Kudu connection for reads cannot be null");
    sessionForReads = apexKuduConnectionForReads.getKuduClient().newSession();
    tableForReads = apexKuduConnectionForReads.getKuduTable();
    transactionidCol = new ColumnSchema.ColumnSchemaBuilder("transactionid", Type.BINARY)
      .key(true)
      .build();
    timestampCol = new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS)
      .key(true)
      .build();

  }

  @Override
  protected boolean isEligibleForPassivationInReconcilingWindow(KuduExecutionContext executionContext,
                                                                long reconcilingWindowId)
  {
    if (tableForReads == null) {
      initConnectionForReads();
    }
    KuduScanner.KuduScannerBuilder scannerBuilder = apexKuduConnectionForReads.getKuduClient()
                                                    .newScannerBuilder(tableForReads);
    KuduExecutionContext<TransactionPayload> executionContextForThisTuple =
      (KuduExecutionContext<TransactionPayload>) executionContext;
    KuduScanner scannerForThisRead = scannerBuilder
                                      .limit(1)
                                      .addPredicate(KuduPredicate.newComparisonPredicate(transactionidCol,
                                       KuduPredicate.ComparisonOp.EQUAL,
                                       executionContextForThisTuple.getPayload().getTransactionId()))
                                      .addPredicate(KuduPredicate.newComparisonPredicate(timestampCol,
                                        KuduPredicate.ComparisonOp.EQUAL,
                                        executionContextForThisTuple.getPayload().getTimestamp()))
                                      .build();
    if (scannerForThisRead.hasMoreRows()) {
      return false;
    } else {
      return true;
    }
  }
}
