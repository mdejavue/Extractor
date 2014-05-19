package de.s9mtmeis.jobs.old;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


	public class ReducerConcatRDFa extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, Context context )
			      throws IOException {

			String completeOutString = "!!START!!\\n";
			
			while (values.hasNext()){
                Text txt = values.next();      
                
                String[] split = txt.toString().split(" .");
                String outString = "";
                
                for ( String s : split ) {
                	if ( s.contains("Offer")) {
		                outString = outString + s;   
                	}
                }
                completeOutString = "start\\n" + outString + "\\nend\\n";
			}
			
			Text out = new Text();
			out.set(completeOutString);
                try {
					context.write(key, out);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}   
             }
		  }		



