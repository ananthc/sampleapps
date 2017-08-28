/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package github.ananthc.sampleapps.apex;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import jep.Jep;
import jep.JepConfig;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class JepBenchmark
{

    @Benchmark @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void testSVMPredictWithConstantParams(JepHandle jepHandle, Blackhole sink, SVMScoringInputData input)
        throws Exception {
      jepHandle.getJepInstance().set("input",input.getNd());
      //jepHandle.jepInstance.eval("res = svmmodel.predict([[4., 4., 6.,8.]])");
      jepHandle.jepInstance.eval("res = svmmodel.predict(input)");
      Object respnseValue = jepHandle.jepInstance.getValue("res");
      sink.consume(respnseValue);
    }


  @State(Scope.Thread)
  public static class JepHandle
  {
    public Jep jepInstance;

    @Setup(Level.Trial)
    public void initJep()
    {
      System.out.println("Lib path = " + System.getProperty("java.library.path"));
      try {
        System.loadLibrary("jep");
        JepConfig config = new JepConfig()
          .setRedirectOutputStreams(true)
          .setInteractive(false)
          .setClassLoader(Thread.currentThread().getContextClassLoader()
          );
        jepInstance = new Jep(config);
        jepInstance.eval("from sklearn.externals import joblib");
        System.out.println("Status of loaded model ... " + jepInstance.eval("svmmodel = joblib.load('/tmp/svmmodel1')"));
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }

    @TearDown(Level.Trial)
    public void closeJep()
    {
      jepInstance.close();
    }


    public Jep getJepInstance()
    {
      return jepInstance;
    }

    public void setJepInstance(Jep jepInstance)
    {
      this.jepInstance = jepInstance;
    }
  }


}
