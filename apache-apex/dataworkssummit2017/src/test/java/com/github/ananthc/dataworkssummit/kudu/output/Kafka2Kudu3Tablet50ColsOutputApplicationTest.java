package com.github.ananthc.dataworkssummit.kudu.output;

import javax.validation.ConstraintViolationException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Created by Ananth on 17/9/17.
 */
public class Kafka2Kudu3Tablet50ColsOutputApplicationTest
{
  @Test
  public void testApp()
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kafka2Kudu3Tablet50ColsOutputApplication(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
