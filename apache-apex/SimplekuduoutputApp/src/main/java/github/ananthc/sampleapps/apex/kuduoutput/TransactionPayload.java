package github.ananthc.sampleapps.apex.kuduoutput;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

/**
 * Created by ananth on 9/04/17.
 */
public class TransactionPayload
{

  private String transactionId;

  private long timestamp;

  private boolean stepUp;

  private String deviceId;

  private Double transactionAmount;

  private char transactionEditMode;

  public String getTransactionId()
  {
    return transactionId;
  }

  public void setTransactionId(String transactionId)
  {
    this.transactionId = transactionId;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public boolean isStepUp()
  {
    return stepUp;
  }

  public void setStepUp(boolean stepUp)
  {
    this.stepUp = stepUp;
  }

  public String getDeviceId()
  {
    return deviceId;
  }

  public void setDeviceId(String deviceId)
  {
    this.deviceId = deviceId;
  }

  public Double getTransactionAmount()
  {
    return transactionAmount;
  }

  public void setTransactionAmount(Double transactionAmount)
  {
    this.transactionAmount = transactionAmount;
  }

  public char getTransactionEditMode()
  {
    return transactionEditMode;
  }

  public void setTransactionEditMode(char transactionEditMode)
  {
    this.transactionEditMode = transactionEditMode;
  }
}
