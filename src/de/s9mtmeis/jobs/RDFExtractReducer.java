package de.s9mtmeis.jobs;

import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



/**
 * Hadoop Reducer. Writes RDf from the extractor and stat entries
 * 
 * @author Steffen Stadtm�ller <steffen.stadtmueller@kit.edu>
 */
public class RDFExtractReducer extends Reducer<Text, Text, Text, Text> {
  
	public void reduce(Text key, Iterator<Text> values, Context context)
	      throws Exception {

	if(key.toString().startsWith("stats4")){
		while(values.hasNext()){
			context.write(key, values.next());
		}
	}else{
		Text empt = new Text("");
		while(values.hasNext()){
			context.write(empt, values.next());
		}
	}
	  
	
  }
}
