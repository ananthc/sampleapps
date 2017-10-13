package github.ananthc.sampleapps.apex.data.input;

import java.util.Random;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import jep.NDArray;

/**
 * Created by Ananth on 13/10/17.
 */
@State(Scope.Thread)
public class MnistInputData
{
  public static Random random = new Random();

  private NDArray<float[]> nd;

  private static final int DIMENSION_SIZE = 28;

  @Setup(Level.Invocation)
  public void setUpDataForInput()
  {
    float[] f = new float[( DIMENSION_SIZE * DIMENSION_SIZE)];
    for ( int i=0; i < (DIMENSION_SIZE * DIMENSION_SIZE ); i++)
    {
      f[i] = random.nextFloat();
    };
    nd = new NDArray<>(f, 1, DIMENSION_SIZE,DIMENSION_SIZE,1);
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
