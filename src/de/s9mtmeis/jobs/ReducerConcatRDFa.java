package de.s9mtmeis.jobs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


	public class ReducerConcatRDFa extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values, Context context )
			      throws IOException {

			
			while (values.hasNext()) {
                Text txt = values.next();
                String inputString = "<start>" + txt.toString() + "<end>";
                Text out = new Text();
                out.set(inputString);
                //System.out.println(inputString);
                try {
					context.write(key, out);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}                
		  }		
		}
	}

