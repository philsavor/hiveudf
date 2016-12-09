package sample.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
  private Text result = new Text();
  
  public Text evaluate( Text action
		  				,Text stime
		  				,Text p1
						,Text ua_model
						,Text net_work) {
    
    StringBuilder builder = new StringBuilder();

    //action
    if(null != action){
    	builder.append("1 " + action + "    ");
    }
    
    //type: stime
    if(null != stime){
    	builder.append("stime:" + stime);
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
    if(null != ua_model)
    	builder.append("3:" + ua_model + " ");
    
    if(null != net_work)
    	builder.append("4:" + net_work + " ");
    
    result.set(builder.toString());
    
    return result;
  }

}