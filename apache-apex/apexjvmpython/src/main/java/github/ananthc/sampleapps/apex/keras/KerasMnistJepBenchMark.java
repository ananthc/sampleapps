package github.ananthc.sampleapps.apex.keras;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import github.ananthc.sampleapps.apex.JepHandle;
import github.ananthc.sampleapps.apex.data.input.MnistInputData;

/**
 * Created by Ananth on 13/10/17.
 */

@State(Scope.Benchmark)
public class KerasMnistJepBenchMark
{


  @Benchmark
  @BenchmarkMode(Mode.AverageTime) @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void testMNISTKerasWithTF(JepHandle jepHandle, Blackhole sink, MnistInputData input)
    throws Exception {
    jepHandle.getJepInstance().set("input",input.getNd());
    jepHandle.getJepInstance().eval("res = loaded_model.predict(input)");
    jep.NDArray responseValue = (jep.NDArray) jepHandle.jepInstance.getValue("res");
    //System.out.println("res=" + ((float[]) responseValue.getData())[0]);
  }

}