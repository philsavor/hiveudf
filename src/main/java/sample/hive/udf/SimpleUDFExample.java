package sample.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;



class SimpleUDFExample extends UDF {
  
  public Text evaluate(Text input) {
    if(input == null) return null;
    return new Text("Hello " + input.toString());
  }
  
  public static void main(String args[]){
	   SimpleUDFExample example = new SimpleUDFExample();
	   System.out.println(example.evaluate(new Text("world")).toString());
  }
}