package weimar.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TopK {
	
	   public static Map<String, Double> sortByValue(Map<String, Double> unsortMap) {

		   Comparator<String> comparator = new ValueComparator<String, Double>(unsortMap);
			Map<String, Double> result = new TreeMap<String, Double>(comparator);
			result.putAll(unsortMap);
		   return result;
	   }
	   
	   
	   public static void saveFinal(String input, String output, FileSystem fs) throws IOException
	   {
		   String line;
	       
		    DecimalFormat df = new DecimalFormat("0");
			df.setMaximumFractionDigits(10);
			
	        Map<String, Double> unsortMap = new HashMap<String, Double>();
			Path pt=new Path(String.valueOf(input)+"/part-r-00000");
			
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	        
	        
	        while((line=br.readLine())!=null){
	        	String arrayStr[] = line.split("\t")[1].split(",");
	        	unsortMap.put(line.split("\t")[1], Double.parseDouble(arrayStr[3]));
	        }
	        
	        Map<String, Double> sortedMap = TopK.sortByValue(unsortMap);
	        
			if (fs.exists(new Path(String.valueOf(output))))
				fs.delete(new Path(String.valueOf(output)), true);
	        
	        Path ptout=new Path(String.valueOf(output)+"/part-r-00000");
	        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(ptout,true)));

	        int idx=1;
	        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
	        	
	            System.out.println("Key : " + entry.getKey()
	                    + " Value : " + df.format(entry.getValue()));
	            bw.write(idx+","+entry.getKey());
	            bw.newLine();
	            idx++;
	        }
	        bw.close();
	   }
	   
	   public static void saveTopK(String input, String output, FileSystem fs, int K) throws IOException
	   {
	        String line;
	       
		    DecimalFormat df = new DecimalFormat("0");
			df.setMaximumFractionDigits(10);
			
	        Map<String, Double> unsortMap = new HashMap<String, Double>();
			Path pt=new Path(String.valueOf(input)+"/part-r-00000");
			
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	        
	        while((line=br.readLine())!=null){
	        	String arrayStr[] = line.split("\t");
	        	unsortMap.put(arrayStr[0], Double.parseDouble(arrayStr[1]));
	        }
	        
	        Map<String, Double> sortedMap = TopK.sortByValue(unsortMap);
	        
			if (fs.exists(new Path(String.valueOf(output))))
				fs.delete(new Path(String.valueOf(output)), true);
	        
	        Path ptout=new Path(String.valueOf(output)+"/part-r-00000");
	        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(ptout,true)));
	        int i = 0;
	        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
	        	if(i == K)
	        		break;
	        	
	            System.out.println("Key : " + entry.getKey()
	                    + " Value : " + df.format(entry.getValue()));
	            bw.write(entry.getKey() + "\t" + df.format(entry.getValue()));
	            bw.newLine();
	            i++;
	        }
	        bw.close();
	   }
	
}

//a comparator using generic type
class ValueComparator<K, V extends Comparable<V>> implements Comparator<K>{

	HashMap<K, V> map = new HashMap<K, V>();

	public ValueComparator(Map<K, V> map){
		this.map.putAll(map);
	}

	@Override
	public int compare(K s1, K s2) {
		return -map.get(s1).compareTo(map.get(s2));//descending order	
	}
}