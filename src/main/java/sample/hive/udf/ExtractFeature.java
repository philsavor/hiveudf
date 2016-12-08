package sample.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
  private Text result = new Text();
  
  public Text evaluate(Text action
		  				,Integer tm
						,Text p1
						,Text contentid) {
    if (p1 == null || contentid == null) {
		  return null;
    }
    
    StringBuilder builder = new StringBuilder();
    //type: action
    if(null != action){
    	builder.append("1 ");
    }
    
    //time
    if(null != tm){
    	builder.append("tm:" + String.valueOf(tm) + " ");
    }
    
    //f1: iphone
    if("2_22_235".equals(p1))
    	builder.append("1:1 ");
    else
    	builder.append("1:0 ");
    
    
    //f2: android
    if("2_22_236".equals(p1))
    	builder.append("2:1 ");
    else
    	builder.append("2:0 ");
    
    //f3: contentid
    if(null != contentid)
    	builder.append("3:" + contentid + " ");
    
    
    result.set(builder.toString());
    
    return result;
  }

}