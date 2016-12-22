package sample.hive.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

public class ExtractUserFeature extends UDF {
	private Text result = new Text();

	//ndays
	private Feature fNDays0 = new Feature("ndays_0",1);
	private Feature fNDays1To2 = new Feature("ndays_1_2",2);
	private Feature fNDays3To4 = new Feature("ndays_3_4",3);
	private Feature fNDays5More = new Feature("ndays_5",4);
	//tm
	private Feature fTM20Less = new Feature("tm_20",5);
	private Feature fTM20To40 = new Feature("tm_20_40",6);
	private Feature fTM40More = new Feature("tm_40",7);

	public Text evaluate( Integer ndays
						,Long tm) {

		StringBuilder builder = new StringBuilder();

			/*
			 * fNDays0
			 * fNDays1To2
			 * fNDays3To4
			 * fNDays5More
			 */
			if(null != ndays){
				switch(ndays){
					case 0:
						fNDays0.setValue("1");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("0");
						break;
					case 1: case 2:
						fNDays0.setValue("0");
						fNDays1To2.setValue("1");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("0");
						break;
					case 3: case 4:
						fNDays0.setValue("0");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("1");
						fNDays5More.setValue("0");
						break;
					case 5: case 6: case 7:
						fNDays0.setValue("0");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("1");
						break;
					default:
						fNDays0.setValue("0");
						fNDays1To2.setValue("0");
						fNDays3To4.setValue("0");
						fNDays5More.setValue("0");
						break;
				}
			}else{
				fNDays0.setValue("0");
				fNDays1To2.setValue("0");
				fNDays3To4.setValue("0");
				fNDays5More.setValue("0");	
			}
			
			/*
			 * fTM20Less
			 * fTM20To40
			 * fTM40More
			 */
			if(null != tm){
				double mins = tm / (60 * 7);
				if(mins < 20){
					fTM20Less.setValue("1");
					fTM20To40.setValue("0");
					fTM40More.setValue("0");
				}else if(mins >= 20 && mins <= 40){
					fTM20Less.setValue("0");
					fTM20To40.setValue("1");
					fTM40More.setValue("0");
				}else{
					fTM20Less.setValue("0");
					fTM20To40.setValue("0");
					fTM40More.setValue("1");
				}
			}else{
				fTM20Less.setValue("1");
				fTM20To40.setValue("0");
				fTM40More.setValue("0");
			}
			
		
		/*
		 *	setup features' order
		 */
		List<Feature> featureList = new ArrayList<Feature>();
		featureList.add(fNDays0);
		featureList.add(fNDays1To2);
		featureList.add(fNDays3To4);
		featureList.add(fNDays5More);
		featureList.add(fTM20Less);
		featureList.add(fTM20To40);
		featureList.add(fTM40More);
		Collections.sort(featureList);

		Map<String,String> fMap = new HashMap<String,String>();
		for(Feature f : featureList){
			fMap.put(f.getKey(), f.getValue());
		}
		String mapAsJson = "";
		try {
			mapAsJson = new ObjectMapper().writeValueAsString(fMap);
		} catch (Exception e) {
			System.out.println("生成json string失败！");
		}
		builder.append(mapAsJson);
		/*
		 *	setup features' order
		 */

		result.set(builder.toString().trim());

		return result;
	}
}