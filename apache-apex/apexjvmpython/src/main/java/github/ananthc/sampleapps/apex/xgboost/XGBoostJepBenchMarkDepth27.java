package github.ananthc.sampleapps.apex.xgboost;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;

import github.ananthc.sampleapps.apex.JepHandle;
import github.ananthc.sampleapps.apex.data.input.IrisScoringInputData;

/**
 * Created by Ananth on 2/10/17.
 */
public class XGBoostJepBenchMarkDepth27
{

  @Benchmark
  @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void testXGBoostPredictIrisDepth27(JepHandle jepHandle, Blackhole sink, IrisScoringInputData input)
    throws Exception {
    jepHandle.getJepInstance().set("input",input.getNd());
    jepHandle.getJepInstance().eval("res = booster27.predict(xgb.DMatrix(input))");
    jep.NDArray responseValue = (jep.NDArray) jepHandle.jepInstance.getValue("res");
    //System.out.println("res=" + ((float[]) responseValue.getData())[0]);
  }

}
