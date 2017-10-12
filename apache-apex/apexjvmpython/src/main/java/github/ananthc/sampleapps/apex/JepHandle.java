package github.ananthc.sampleapps.apex;

import java.io.File;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import org.apache.commons.io.FileUtils;

import jep.Jep;
import jep.JepConfig;

/**
 * Created by Ananth on 2/10/17.
 */
@State(Scope.Thread)
public class JepHandle
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
      jepInstance.eval("import pickle");
      jepInstance.eval("import sys");
      jepInstance.eval("import platform");
      jepInstance.eval("import numpy as np");
      jepInstance.eval("import xgboost as xgb");
      jepInstance.eval("print(platform.python_version())");
      jepInstance.eval("import keras");
      jepInstance.eval("from keras.models import Sequential, import model_from_json");
      jepInstance.eval("from keras.layers import Dense, Dropout, Flatten, Conv2D, MaxPooling2D");
      jepInstance.eval("from keras import backend as K");
      jepInstance.eval("import h5py");
      loadSVMModel();
      loadXGBoostDepth3Model();
      loadXGBoostDepth9Model();
      loadXGBoostDepth27Model();
      loadXGBoostDepth125Model();
      loadKerasMNISTModel();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private void migrateFileFromResourcesFolderToTemp(String resourceFileName,String targetFilePath) throws Exception
  {

    ClassLoader classLoader = getClass().getClassLoader();
    File outFile = new File(targetFilePath);
    FileUtils.copyInputStreamToFile(classLoader.getResourceAsStream(resourceFileName), outFile);
  }

  private void loadSVMModel() throws Exception
  {
    String modelPath = "/tmp/svmmodel1";
    String resourceFileName = "svm/svm-iris.pickle";
    migrateFileFromResourcesFolderToTemp(resourceFileName,modelPath);
    jepInstance.eval("fileHandle = open('" + modelPath+"','rb')");
    System.out.println("Status of loaded model ... " + jepInstance.eval("svmmodel = pickle.load(fileHandle)"));
    jepInstance.eval("fileHandle.close()");
  }

  private void loadXGBoostDepth3Model() throws Exception
  {
    String modelPath = "/tmp/xgbboost-depth3.bin";
    String resourceFileName = "xgboost/xgboost-60-trees-depth-3.bin";
    migrateFileFromResourcesFolderToTemp(resourceFileName,modelPath);
    jepInstance.eval("booster3 = xgb.Booster()");
    jepInstance.eval("booster3.load_model('" + modelPath + "')");
  }

  private void loadXGBoostDepth9Model() throws Exception
  {
    String modelPath = "/tmp/xgbboost-depth6.bin";
    String resourceFileName = "xgboost/xgboost-120-trees-depth-9.bin";
    migrateFileFromResourcesFolderToTemp(resourceFileName,modelPath);
    jepInstance.eval("booster9 = xgb.Booster()");
    jepInstance.eval("booster9.load_model('" + modelPath + "')");
  }

  private void loadXGBoostDepth27Model() throws Exception
  {
    String modelPath = "/tmp/xgbboost-depth27.bin";
    String resourceFileName = "xgboost/xgboost-300-trees-depth-27.bin";
    migrateFileFromResourcesFolderToTemp(resourceFileName,modelPath);
    jepInstance.eval("booster27 = xgb.Booster()");
    jepInstance.eval("booster27.load_model('" + modelPath + "')");
  }


  private void loadXGBoostDepth125Model() throws Exception
  {
    String modelPath = "/tmp/xgbboost-depth125.bin";
    String resourceFileName = "xgboost/xgboost-900-trees-depth-125.bin";
    migrateFileFromResourcesFolderToTemp(resourceFileName,modelPath);
    jepInstance.eval("booster125 = xgb.Booster()");
    jepInstance.eval("booster125.load_model('" + modelPath + "')");
  }


  private void loadKerasMNISTModel() throws Exception
  {
    String h5ModelWeightsPath = "/tmp/keras-minst-model-jep.h5";
    String jsonModelPath = "/tmp/keras-minst-model-jep.json";
    String resourceFileNameForModelWeights = "keras/keras-minst-model.h5";
    String resourceFileNameForModel = "keras/keras-minst-model.json";
    migrateFileFromResourcesFolderToTemp(resourceFileNameForModelWeights,h5ModelWeightsPath);
    migrateFileFromResourcesFolderToTemp(resourceFileNameForModel,jsonModelPath);
    jepInstance.eval("json_file = open(\""+jsonModelPath + "\", 'r')");
    jepInstance.eval("loaded_model_json = json_file.read()");
    jepInstance.eval("json_file.close()");
    jepInstance.eval("loaded_model = model_from_json(loaded_model_json)");
    jepInstance.eval("loaded_model.load_weights(\"" + h5ModelWeightsPath +"\")");
    jepInstance.eval("print(\"Loaded model from disk\")");
    jepInstance.eval("print(\"Loaded model from disk\")");
    jepInstance.eval("loaded_model.compile(loss='categorical_crossentropy', optimizer='adadelta', metrics=['accuracy'])");
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
