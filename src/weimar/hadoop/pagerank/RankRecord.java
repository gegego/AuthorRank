package weimar.hadoop.pagerank;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RankRecord {
	public String sourceID;
	public double rankValue;
	public int weight;
	public Map<String, Integer> targetList;
	
	public RankRecord(String strLine){
		String[] strArray = strLine.split("#");
		sourceID = strArray[0].split("\t")[0];
		
		String strVal[] = strArray[0].split("\t")[1].split("@");
		weight = Integer.parseInt(strVal[0]);
		rankValue = Double.parseDouble(strVal[1]);
		
		targetList = new HashMap<String, Integer>();
		for (int i=1;i<strArray.length;i++){
			String val = strArray[i];
			String[] strPair = val.split("@");
			
			targetList.put(strPair[0], Integer.parseInt(strPair[1]));
		}
	}
}
