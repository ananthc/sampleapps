package com.github.ananthc.dataworkssummit.kudu.input;

import javax.validation.ConstraintViolationException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.github.ananthc.dataworkssummit.kudu.output.Kafka2Kudu3Tablet50ColsOutputApplication;

import com.datatorrent.api.LocalMode;

/**
 * Created by Ananth on 17/9/17.
 */
public class Kudu3Tablets50ColsInputAppTest
{

  @Test
  public void testApp()
  {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Kudu3Tablets50ColsInputApp(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(1000000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      System.out.println(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
