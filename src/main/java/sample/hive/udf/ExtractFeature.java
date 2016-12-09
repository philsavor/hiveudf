package sample.hive.udf;

import java.text.MessageFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
  private Text result = new Text();
  
  private final String DATA_P1_IPHONE = "2_22_235";
  private final String DATA_P1_ANDROID = "2_22_236";
  private final String DATA_NETWORK_1 = "1";
  private final String DATA_NETWORK_2 = "2";
  private final String DATA_NETWORK_3 = "3";
  private final String DATA_NETWORK_4 = "4";
  
  //format
  private final String FEATURE_STRING_FORMAT = "{0}:{1}";
  
  //cls
  private String cls;
  //stime
  private String f_stime_0_6,f_stime_0_6_seq = "0";
  private String f_stime_6_9,f_stime_6_9_seq = "1";
  private String f_stime_9_12,f_stime_9_12_seq = "2";
  private String f_stime_12_15,f_stime_12_15_seq = "3";
  private String f_stime_15_18,f_stime_15_18_seq = "4";
  private String f_stime_18_23,f_stime_18_23_seq = "5";
  private String f_stime_other,f_stime_other_seq = "6";
  //p1
  private String f_p1_iphone,f_p1_iphone_seq = "7";
  private String f_p1_android,f_p1_android_seq = "8";
  //net_work
  private String f_net_work_1,f_net_work_1_seq = "9";
  private String f_net_work_2,f_net_work_2_seq = "10";
  private String f_net_work_3,f_net_work_3_seq = "11";
  private String f_net_work_4,f_net_work_4_seq = "12";
  private String f_net_work_other,f_net_work_other_seq = "13";
  
  public Text evaluate( Text action
		  				,Text stime
		  				,Text p1
						,Text ua_model
						,Text net_work) {
    
    StringBuilder builder = new StringBuilder();

    //action (reveal,content)
    if(null != action){
    	cls = "1";
    }else
    	cls = "0";
    
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
    }else{
		f_stime_0_6 = "0";
		f_stime_6_9 = "0";
		f_stime_9_12 = "0";
		f_stime_12_15 = "0";
		f_stime_15_18 = "0";
		f_stime_18_23 = "0";
		f_stime_other = "1";
    }
    
    //f1: iphone
    if(DATA_P1_IPHONE.equals(p1))
    	f_p1_iphone = "1";
    else
    	f_p1_iphone = "0";
    
    
    //f2: android
    if(DATA_P1_ANDROID.equals(p1))
    	f_p1_android = "1";
    else
    	f_p1_android = "0";
    
    
    if(null != net_work){
    	if(DATA_NETWORK_1.equals(net_work)){
    		f_net_work_1 = "1";
    		f_net_work_2 = "0";
    		f_net_work_3 = "0";
    		f_net_work_4 = "0";
    		f_net_work_other = "0";
    	}else if(DATA_NETWORK_2.equals(net_work)){
    		f_net_work_1 = "0";
    		f_net_work_2 = "1";
    		f_net_work_3 = "0";
    		f_net_work_4 = "0";
    		f_net_work_other = "0";
    	}else if(DATA_NETWORK_3.equals(net_work)){
    		f_net_work_1 = "0";
    		f_net_work_2 = "0";
    		f_net_work_3 = "1";
    		f_net_work_4 = "0";
    		f_net_work_other = "0";
    	}else if(DATA_NETWORK_4.equals(net_work)){
    		f_net_work_1 = "0";
    		f_net_work_2 = "0";
    		f_net_work_3 = "0";
    		f_net_work_4 = "1";
    		f_net_work_other = "0";
    	}else{
    		f_net_work_1 = "0";
    		f_net_work_2 = "0";
    		f_net_work_3 = "0";
    		f_net_work_4 = "0";
    		f_net_work_other = "1";
    	}
    }else{
		f_net_work_1 = "0";
		f_net_work_2 = "0";
		f_net_work_3 = "0";
		f_net_work_4 = "0";
		f_net_work_other = "0";
    }
    
    /*
     *	setup features' sequence
     */
    builder.append(cls + " "); 
    builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_0_6_seq, f_stime_0_6) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_6_9_seq, f_stime_6_9) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_9_12_seq, f_stime_9_12) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_12_15_seq, f_stime_12_15) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_15_18_seq, f_stime_15_18) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_18_23_seq, f_stime_18_23) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_stime_other_seq, f_stime_other) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_p1_iphone_seq, f_p1_iphone) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_p1_android_seq, f_p1_android) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_net_work_1_seq, f_net_work_1) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_net_work_2_seq, f_net_work_2) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_net_work_3_seq, f_net_work_3) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_net_work_4_seq, f_net_work_4) + " ");
	builder.append(MessageFormat.format(FEATURE_STRING_FORMAT, f_net_work_other_seq, f_net_work_other));
    /*
     *	setup features' sequence
     */
    
    result.set(builder.toString());
    
    return result;
  }

}