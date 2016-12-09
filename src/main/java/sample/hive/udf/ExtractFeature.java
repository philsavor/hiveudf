package sample.hive.udf;

import java.text.MessageFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
  private Text result = new Text();
  
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
  private String f_net_work_4g,f_net_work_4g_seq = "9";
  private String f_net_work_3g,f_net_work_3g_seq = "10";
  private String f_net_work_2g,f_net_work_2g_seq = "11";
  private String f_net_work_wifi,f_net_work_wifi_seq = "12";
  private String f_net_work_other,f_net_work_other_seq = "13";
  
  public Text evaluate( Text action
		  				,Text stime
		  				,Text p1
						,Text ua_model
						,Text net_work) {
    
    StringBuilder builder = new StringBuilder();

    /*
     * cls
     */
    if(null != action){
    	String actionString = action.toString();
    	switch(actionString){
    		case ExtractFeatureConstant.DATA_ACTION_CONTENT:
    		case ExtractFeatureConstant.DATA_ACTION_SHARE:
    		case ExtractFeatureConstant.DATA_ACTION_COMMENT:
    		case ExtractFeatureConstant.DATA_ACTION_COLLECT:
    		case ExtractFeatureConstant.DATA_ACTION_LIKE:
    			cls = "1";
    			break;
    		case ExtractFeatureConstant.DATA_ACTION_NEGATIVE_FEEDBACK:
    		case ExtractFeatureConstant.DATA_ACTION_CANCEL_LIKE:
    		case ExtractFeatureConstant.DATA_ACTION_REVEAL:
    			cls = "0";
    			break;
    		default:
    			cls = "0";
    			break;
    	}
    }else
    	cls = "0";
    
    /*
     * f_stime_0_6
     * f_stime_6_9
     * f_stime_9_12
     * f_stime_12_15
     * f_stime_15_18
     * f_stime_18_23
     * f_stime_other
     */
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
    
    /*
     * f_p1_iphone
     */
    String p1String = p1.toString();
    
    if(ExtractFeatureConstant.DATA_P1_IPHONE.equals(p1String))
    	f_p1_iphone = "1";
    else
    	f_p1_iphone = "0";
    
    
    /*
     * f_p1_android
     */
    if(ExtractFeatureConstant.DATA_P1_ANDROID.equals(p1String))
    	f_p1_android = "1";
    else
    	f_p1_android = "0";
    
    /*
     * f_net_work_4g
     * f_net_work_3g
     * f_net_work_2g
     * f_net_work_wifi
     * f_net_work_other
     */
    if(null != net_work){
		String netWorkString = net_work.toString();
		switch(netWorkString){
			case ExtractFeatureConstant.DATA_NETWORK_LTE:
				f_net_work_4g = "1";
				f_net_work_3g = "0";
				f_net_work_2g = "0";
				f_net_work_wifi = "0";
				f_net_work_other = "0";
				break;
			case ExtractFeatureConstant.DATA_NETWORK_UMTS:
			case ExtractFeatureConstant.DATA_NETWORK_HSDPA:
			case ExtractFeatureConstant.DATA_NETWORK_HSUPA:
			case ExtractFeatureConstant.DATA_NETWORK_HSPA:
			case ExtractFeatureConstant.DATA_NETWORK_CDMA:
			case ExtractFeatureConstant.DATA_NETWORK_EVDO_0:
			case ExtractFeatureConstant.DATA_NETWORK_EVDO_A:
			case ExtractFeatureConstant.DATA_NETWORK_HSPAP:
				f_net_work_4g = "0";
				f_net_work_3g = "1";
				f_net_work_2g = "0";
				f_net_work_wifi = "0";
				f_net_work_other = "0";
				break;
			case ExtractFeatureConstant.DATA_NETWORK_GPRS:
			case ExtractFeatureConstant.DATA_NETWORK_EDGE:
			case ExtractFeatureConstant.DATA_NETWORK_1XRTT:
				f_net_work_4g = "0";
				f_net_work_3g = "0";
				f_net_work_2g = "1";
				f_net_work_wifi = "0";
				f_net_work_other = "0";
				break;
			case ExtractFeatureConstant.DATA_NETWORK_WIFI:
			case ExtractFeatureConstant.DATA_NETWORK_ETHERNET:
				f_net_work_4g = "0";
				f_net_work_3g = "0";
				f_net_work_2g = "0";
				f_net_work_wifi = "1";
				f_net_work_other = "0";
				break;
			case ExtractFeatureConstant.DATA_NETWORK_NONETWORK_ANDROID:
			case ExtractFeatureConstant.DATA_NETWORK_NONETWORK:
				f_net_work_4g = "0";
				f_net_work_3g = "0";
				f_net_work_2g = "0";
				f_net_work_wifi = "0";
				f_net_work_other = "1";
				break;
			default:
				f_net_work_4g = "0";
				f_net_work_3g = "0";
				f_net_work_2g = "0";
				f_net_work_wifi = "0";
				f_net_work_other = "1";
				break;
		}
	}else{
		f_net_work_4g = "0";
		f_net_work_3g = "0";
		f_net_work_2g = "0";
		f_net_work_wifi = "0";
		f_net_work_other = "1";
    }
    
    /*
     *	setup features' sequence
     */
    builder.append(cls + " "); 
    builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_0_6_seq, f_stime_0_6) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_6_9_seq, f_stime_6_9) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_9_12_seq, f_stime_9_12) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_12_15_seq, f_stime_12_15) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_15_18_seq, f_stime_15_18) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_18_23_seq, f_stime_18_23) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_stime_other_seq, f_stime_other) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_p1_iphone_seq, f_p1_iphone) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_p1_android_seq, f_p1_android) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_net_work_4g_seq, f_net_work_4g) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_net_work_3g_seq, f_net_work_3g) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_net_work_2g_seq, f_net_work_2g) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_net_work_wifi_seq, f_net_work_wifi) + " ");
	builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f_net_work_other_seq, f_net_work_other));
    /*
     *	setup features' sequence
     */
    
    result.set(builder.toString());
    
    return result;
  }

}