package github.ananthc.sampleapps.apex;

import java.util.Random;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import jep.NDArray;

/**
 * Created by Ananth on 29/8/17.
 */
@State(Scope.Thread)
public class SVMScoringInputData
{

  public static Random random = new Random();

  private NDArray<float[]> nd;

  @Setup(Level.Invocation)
  public void setUpDataForInput()
  {
    float[] f = new float[] { random.nextFloat()*10 , random.nextFloat()*10, random.nextFloat()*10,
        random.nextFloat()*10  };
    nd = new NDArray<>(f, 1, 4);
  }

  public NDArray<float[]> getNd()
  {
    return nd;
  }

  public void setNd(NDArray<float[]> nd)
  {
    this.nd = nd;
  }
}
