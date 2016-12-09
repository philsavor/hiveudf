package sample.hive.udf;

import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
  private Text result = new Text();
  
  private String f_stime_0_6,f_stime_6_9,f_stime_9_12,f_stime_12_15,f_stime_15_18,f_stime_18_23,f_stime_other;
  
  public Text evaluate( Text action
		  				,Text stime
		  				,Text p1
						,Text ua_model
						,Text net_work) {
    
    StringBuilder builder = new StringBuilder();

    //action (reveal,content)
    if(null != action){
    	builder.append("1 " + action + "    ");
    }
    
    //type: stime (1262307115161)
    if(null != stime){
		Long timestamp = Long.parseLong(stime.toString());
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		int hour = Calendar.HOUR;
		switch(hour){
			case 0: case 1: case 2: case 3: case 4: case 5: case 6:
				f_stime_0_6 = "1";
				f_stime_6_9 = "0";
				f_stime_9_12 = "0";
				f_stime_12_15 = "0";
				f_stime_15_18 = "0";
				f_stime_18_23 = "0";
				f_stime_other = "0";
				break;
			case 7: case 8: case 9: 
				f_stime_0_6 = "0";
				f_stime_6_9 = "1";
				f_stime_9_12 = "0";
				f_stime_12_15 = "0";
				f_stime_15_18 = "0";
				f_stime_18_23 = "0";
				f_stime_other = "0";
				break;
			case 10: case 11: case 12: 
				f_stime_0_6 = "0";
				f_stime_6_9 = "0";
				f_stime_9_12 = "1";
				f_stime_12_15 = "0";
				f_stime_15_18 = "0";
				f_stime_18_23 = "0";
				f_stime_other = "0";
				break;
			case 13: case 14: case 15: 
				f_stime_0_6 = "0";
				f_stime_6_9 = "0";
				f_stime_9_12 = "0";
				f_stime_12_15 = "1";
				f_stime_15_18 = "0";
				f_stime_18_23 = "0";
				f_stime_other = "0";
				break;
			case 16: case 17: case 18: 
				f_stime_0_6 = "0";
				f_stime_6_9 = "0";
				f_stime_9_12 = "0";
				f_stime_12_15 = "0";
				f_stime_15_18 = "1";
				f_stime_18_23 = "0";
				f_stime_other = "0";
				break;
			case 19: case 20: case 21: case 22: case 23:
				f_stime_0_6 = "0";
				f_stime_6_9 = "0";
				f_stime_9_12 = "0";
				f_stime_12_15 = "0";
				f_stime_15_18 = "0";
				f_stime_18_23 = "1";
				f_stime_other = "0";
				break;
			default:
				f_stime_0_6 = "0";
				f_stime_6_9 = "0";
				f_stime_9_12 = "0";
				f_stime_12_15 = "0";
				f_stime_15_18 = "0";
				f_stime_18_23 = "0";
				f_stime_other = "1";
				break;
		}

    	builder.append("f_stime_0_6:" + f_stime_0_6 + " ");
    	builder.append("f_stime_6_9:" + f_stime_6_9 + " ");
    	builder.append("f_stime_9_12:" + f_stime_9_12 + " ");
    	builder.append("f_stime_12_15:" + f_stime_12_15 + " ");
    	builder.append("f_stime_15_18:" + f_stime_15_18 + " ");
    	builder.append("f_stime_18_23:" + f_stime_18_23 + " ");
    	builder.append("f_stime_other:" + f_stime_other + " ");
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