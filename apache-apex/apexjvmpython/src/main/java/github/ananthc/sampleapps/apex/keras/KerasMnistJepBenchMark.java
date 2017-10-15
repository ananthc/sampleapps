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
    float[] responseAsFloatArray = (float[]) responseValue.getData();
//    System.out.println("res=[" + (responseAsFloatArray[0]) + "," + responseAsFloatArray[1]
//      + "," + responseAsFloatArray[2] + "," + responseAsFloatArray[3] + "," + responseAsFloatArray[4] + "," +
//      responseAsFloatArray[5]+ "," + responseAsFloatArray[6]+ "," + responseAsFloatArray[7]+ "," + responseAsFloatArray[8]
//      + "," + responseAsFloatArray[9]);
  }

}