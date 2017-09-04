package com.github.ananthc.dataworkssummit.datagenerator;

import java.io.File;
import java.io.FileWriter;

/**
 * Created by Ananth on 31/8/17.
 */
public class ClassSourceCodeGenerator
{

  private String getClassTemplate(String className)
  {
    return "" +
      "package com.github.ananthc.dataworkssummit.pojos;\n" +
      "\n" +
      "\n" +
      "public class "+ className + "\n" +
      "{\n" +
      "\n" +
      "  private int intRowKey;\n" +
      "  \n" +
      "  private long timestampRowKey;" +
      "\n\n\n" +
      "  public int getIntRowKey()\n" +
      "  {\n" +
      "    return intRowKey;\n" +
      "  }\n" +
      "\n" +
      "  public void setIntRowKey(int intRowKey)\n" +
      "  {\n" +
      "    this.intRowKey = intRowKey;\n" +
      "  }\n" +
      "\n" +
      "  public long getTimestampRowKey()\n" +
      "  {\n" +
      "    return timestampRowKey;\n" +
      "  }\n" +
      "\n" +
      "  public void setTimestampRowKey(long timestampRowKey)\n" +
      "  {\n" +
      "    this.timestampRowKey = timestampRowKey;\n" +
      "  }" +
      "\n";

  }

  private String buildClassBody(String className, int numIntCols,int numFloatCols,int numStrCols)
  {
    StringBuffer bffrForCode = new StringBuffer(getClassTemplate(className));
    for ( int i= 0; i < numIntCols; i++) {
      bffrForCode.append("\tprivate int int"+i+";\n");
    }
    for ( int i= 0; i < numFloatCols; i++) {
      bffrForCode.append("\tprivate float float"+i+";\n\n");
    }
    for ( int i= 0; i < numStrCols; i++) {
      bffrForCode.append("\tprivate String str"+i+";\n\n");
    }
    for ( int i= 0; i < numIntCols; i++) {
      bffrForCode.append(""+
        "\tpublic int getInt" + i + "()\n" +
        "\t{\n" +
        "\t\treturn int" + i + ";\n" +
        "\t}\n" +
        "\n\n"+
        "\tpublic void setInt"+ i + "(int int"+ i+")\n"+
        "\t{\n" +
        "\t\tthis.int"+i+" = int"+i+";\n" +
        "\t}\n" +
        "\n");
    }
    for ( int i= 0; i < numFloatCols; i++) {
      bffrForCode.append(""+
        "\tpublic float getFloat" + i + "()\n" +
        "\t{\n" +
        "\t\treturn float" + i + ";\n" +
        "\t}\n" +
        "\n\n"+
        "\tpublic void setFloat"+ i + "(float float"+ i+")\n"+
        "\t{\n" +
        "\t\tthis.float"+i+" = float"+i+";\n" +
        "\t}\n" +
        "\n");
    }
    for ( int i= 0; i < numStrCols; i++) {
      bffrForCode.append(""+
        "\tpublic String getStr" + i + "()\n" +
        "\t{\n" +
        "\t\treturn str" + i + ";\n" +
        "\t}\n" +
        "\n\n"+
        "\tpublic void setStr"+ i + "(String str"+ i+")\n"+
        "\t{\n" +
        "\t\tthis.str"+i+" = str"+i+";\n" +
        "\t}\n" +
        "\n");
    }
    bffrForCode.append("}\n");
    return bffrForCode.toString();
  }



  public void generate50ColPojo() throws Exception
  {
    String fileAndClassName = "FiftyColsPojo";
    int numIntCols = 30;
    int numFloatCols = 8;
    int numStringCols = 10;
    String classBody  = buildClassBody(fileAndClassName,numIntCols,numFloatCols,numStringCols);
    File newFile = new File("/tmp/"+fileAndClassName+".java");
    FileWriter writer = new FileWriter(newFile,false);
    writer.write(classBody);
    writer.close();
  }

  public void generate25ColPojo() throws Exception
  {
    String fileAndClassName = "TwentyFiveColsPojo";
    int numIntCols = 12;
    int numFloatCols = 3;
    int numStringCols = 8;
    String classBody  = buildClassBody(fileAndClassName,numIntCols,numFloatCols,numStringCols);
    File newFile = new File("/tmp/"+fileAndClassName+".java");
    FileWriter writer = new FileWriter(newFile,false);
    writer.write(classBody);
    writer.close();
  }


  public void generate100ColPojo() throws Exception
  {
    String fileAndClassName = "HundredColsPojo";
    int numIntCols = 60;
    int numFloatCols = 18;
    int numStringCols = 20;
    String classBody  = buildClassBody(fileAndClassName,numIntCols,numFloatCols,numStringCols);
    File newFile = new File("/tmp/"+fileAndClassName+".java");
    FileWriter writer = new FileWriter(newFile,false);
    writer.write(classBody);
    writer.close();
  }


  public static void main(String[] args) throws Exception
  {
    ClassSourceCodeGenerator gen = new ClassSourceCodeGenerator();
    gen.generate25ColPojo();
    gen.generate50ColPojo();
    gen.generate100ColPojo();
  }
}
