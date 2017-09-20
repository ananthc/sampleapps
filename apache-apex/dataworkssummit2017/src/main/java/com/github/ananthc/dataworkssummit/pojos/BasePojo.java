package com.github.ananthc.dataworkssummit.pojos;

/**
 * Created by Ananth on 4/9/17.
 */
public class BasePojo
{
  private int intRowKey;

  private long timestampRowKey;

  public BasePojo()
  {
  }

  public int getIntRowKey()
  {
    return intRowKey;
  }

  public void setIntRowKey(int intRowKey)
  {
    this.intRowKey = intRowKey;
  }

  public long getTimestampRowKey()
  {
    return timestampRowKey;
  }

  public void setTimestampRowKey(long timestampRowKey)
  {
    this.timestampRowKey = timestampRowKey;
  }

}
