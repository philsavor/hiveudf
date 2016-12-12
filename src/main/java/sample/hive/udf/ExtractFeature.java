package sample.hive.udf;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ExtractFeature extends UDF {
  private Text result = new Text();
  
  //cls
  private String cls;
  //stime
  private Feature fStime0To6 = new Feature(0);
  private Feature fStime6To9 = new Feature(1);
  private Feature fStime9To12 = new Feature(2);
  private Feature fStime12To15 = new Feature(3);
  private Feature fStime15To18 = new Feature(4);
  private Feature fStime18To23 = new Feature(5);
  private Feature fStimeOther = new Feature(6);
  //p1
  private Feature fP1IPhone = new Feature(7);
  private Feature fP1Android = new Feature(8);
  //net_work
  private Feature fNetwork4g = new Feature(9);
  private Feature fNetwork3g = new Feature(10);
  private Feature fNetwork2g = new Feature(11);
  private Feature fNetworkWifi = new Feature(12);
  private Feature fNetworkOther = new Feature(13);
  
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
     * fStime0To6
     * fStime6To9
     * fStime9To12
     * fStime12To15
     * fStime15To18
     * fStime18To23
     * fStimeOther
     */
    if(null != stime){
		Long timestamp = Long.parseLong(stime.toString());
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timestamp);
		int hour = Calendar.HOUR;
		switch(hour){
			case 0: case 1: case 2: case 3: case 4: case 5: case 6:
				fStime0To6.setValue("1");
				fStime6To9.setValue("0");
				fStime9To12.setValue("0");
				fStime12To15.setValue("0");
				fStime15To18.setValue("0");
				fStime18To23.setValue("0");
				fStimeOther.setValue("0");
				break;
			case 7: case 8: case 9: 
				fStime0To6.setValue("0");
				fStime6To9.setValue("1");
				fStime9To12.setValue("0");
				fStime12To15.setValue("0");
				fStime15To18.setValue("0");
				fStime18To23.setValue("0");
				fStimeOther.setValue("0");
				break;
			case 10: case 11: case 12: 
				fStime0To6.setValue("0");
				fStime6To9.setValue("0");
				fStime9To12.setValue("1");
				fStime12To15.setValue("0");
				fStime15To18.setValue("0");
				fStime18To23.setValue("0");
				fStimeOther.setValue("0");
				break;
			case 13: case 14: case 15: 
				fStime0To6.setValue("0");
				fStime6To9.setValue("0");
				fStime9To12.setValue("0");
				fStime12To15.setValue("1");
				fStime15To18.setValue("0");
				fStime18To23.setValue("0");
				fStimeOther.setValue("0");
				break;
			case 16: case 17: case 18: 
				fStime0To6.setValue("0");
				fStime6To9.setValue("0");
				fStime9To12.setValue("0");
				fStime12To15.setValue("0");
				fStime15To18.setValue("1");
				fStime18To23.setValue("0");
				fStimeOther.setValue("0");
				break;
			case 19: case 20: case 21: case 22: case 23:
				fStime0To6.setValue("0");
				fStime6To9.setValue("0");
				fStime9To12.setValue("0");
				fStime12To15.setValue("0");
				fStime15To18.setValue("0");
				fStime18To23.setValue("1");
				fStimeOther.setValue("0");
				break;
			default:
				fStime0To6.setValue("0");
				fStime6To9.setValue("0");
				fStime9To12.setValue("0");
				fStime12To15.setValue("0");
				fStime15To18.setValue("0");
				fStime18To23.setValue("0");
				fStimeOther.setValue("1");
				break;
		}
    }else{
		fStime0To6.setValue("0");
		fStime6To9.setValue("0");
		fStime9To12.setValue("0");
		fStime12To15.setValue("0");
		fStime15To18.setValue("0");
		fStime18To23.setValue("0");
		fStimeOther.setValue("1");
    }
    
    /*
     * fP1IPhone
     * fP1Android
     */
    String p1String = p1.toString();
    if(ExtractFeatureConstant.DATA_P1_IPHONE.equals(p1String))
    	fP1IPhone.setValue("1");
    else
    	fP1IPhone.setValue("0");
    
    if(ExtractFeatureConstant.DATA_P1_ANDROID.equals(p1String))
    	fP1Android.setValue("1");
    else
    	fP1Android.setValue("0");
    
    /*
     * fNetwork4g
     * fNetwork3g
     * fNetwork2g
     * fNetworkWifi
     * fNetworkOther
     */
    if(null != net_work){
		String netWorkString = net_work.toString();
		switch(netWorkString){
			case ExtractFeatureConstant.DATA_NETWORK_LTE:
				fNetwork4g.setValue("1");
				fNetwork3g.setValue("0");
				fNetwork2g.setValue("0");
				fNetworkWifi.setValue("0");
				fNetworkOther.setValue("0");
				break;
			case ExtractFeatureConstant.DATA_NETWORK_UMTS:
			case ExtractFeatureConstant.DATA_NETWORK_HSDPA:
			case ExtractFeatureConstant.DATA_NETWORK_HSUPA:
			case ExtractFeatureConstant.DATA_NETWORK_HSPA:
			case ExtractFeatureConstant.DATA_NETWORK_CDMA:
			case ExtractFeatureConstant.DATA_NETWORK_EVDO_0:
			case ExtractFeatureConstant.DATA_NETWORK_EVDO_A:
			case ExtractFeatureConstant.DATA_NETWORK_HSPAP:
				fNetwork4g.setValue("0");
				fNetwork3g.setValue("1");
				fNetwork2g.setValue("0");
				fNetworkWifi.setValue("0");
				fNetworkOther.setValue("0");
				break;
			case ExtractFeatureConstant.DATA_NETWORK_GPRS:
			case ExtractFeatureConstant.DATA_NETWORK_EDGE:
			case ExtractFeatureConstant.DATA_NETWORK_1XRTT:
				fNetwork4g.setValue("0");
				fNetwork3g.setValue("0");
				fNetwork2g.setValue("1");
				fNetworkWifi.setValue("0");
				fNetworkOther.setValue("0");
				break;
			case ExtractFeatureConstant.DATA_NETWORK_WIFI:
			case ExtractFeatureConstant.DATA_NETWORK_ETHERNET:
				fNetwork4g.setValue("0");
				fNetwork3g.setValue("0");
				fNetwork2g.setValue("0");
				fNetworkWifi.setValue("1");
				fNetworkOther.setValue("0");
				break;
			case ExtractFeatureConstant.DATA_NETWORK_NONETWORK_ANDROID:
			case ExtractFeatureConstant.DATA_NETWORK_NONETWORK:
				fNetwork4g.setValue("0");
				fNetwork3g.setValue("0");
				fNetwork2g.setValue("0");
				fNetworkWifi.setValue("0");
				fNetworkOther.setValue("1");
				break;
			default:
				fNetwork4g.setValue("0");
				fNetwork3g.setValue("0");
				fNetwork2g.setValue("0");
				fNetworkWifi.setValue("0");
				fNetworkOther.setValue("1");
				break;
		}
	}else{
		fNetwork4g.setValue("0");
		fNetwork3g.setValue("0");
		fNetwork2g.setValue("0");
		fNetworkWifi.setValue("0");
		fNetworkOther.setValue("1");
    }
    
    /*
     *	setup features' sequence
     */
    List<Feature> featureList = new ArrayList<Feature>();
    featureList.add(fStime0To6);
    featureList.add(fStime6To9);
    featureList.add(fStime9To12);
    featureList.add(fStime12To15);
    featureList.add(fStime15To18);
    featureList.add(fStime18To23);
    featureList.add(fStimeOther);
    featureList.add(fP1IPhone);
    featureList.add(fP1Android);
    featureList.add(fNetwork4g);
    featureList.add(fNetwork3g);
    featureList.add(fNetwork2g);
    featureList.add(fNetworkWifi);
    featureList.add(fNetworkOther);
    Collections.sort(featureList);
    
    builder.append(cls + " "); 
    for(Feature f : featureList){
    	if(f.getValue() != null)
    		builder.append(MessageFormat.format(ExtractFeatureConstant.FEATURE_STRING_FORMAT, f.getIndex(), f.getValue()) + " ");
    }
    /*
     *	setup features' sequence
     */
    
    result.set(builder.toString().trim());
    
    return result;
  }

}