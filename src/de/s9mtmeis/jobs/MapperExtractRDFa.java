package de.s9mtmeis.jobs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.any23.Any23;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;
import com.sun.jndi.toolkit.url.Uri;


public class MapperExtractRDFa {
	

	private static final Logger LOG = Logger.getLogger(MapperExtractRDFa.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	
public static class ExtractRDFa extends Mapper<LongWritable, WARCWritable, Text, Text> {

	private Text outKey = new Text();
	private Text outVal = new Text();
	
	
	@Override
	public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
		try {

			if ( value.getRecord().getHeader().getContentType().equals("application/http; msgtype=response")) {
		        //Get the text content as a string.
				byte[] rawData = value.getRecord().getContent();
				String pageText = new String(rawData);
				
				context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
				
				
				Pattern pattern = Pattern.compile("(property|typeof|about|resource)\\s*=");
				Matcher matcher = pattern.matcher(pageText);
				
				
				if (matcher.find()) {    
					// found RDFa pattern
					
					/*1*/ Any23 runner = new Any23();
					/*4*/ DocumentSource source = new StringDocumentSource(pageText, value.getRecord().getHeader().getTargetURI(), value.getRecord().getHeader().getContentType());
					/*5*/ ByteArrayOutputStream out = new ByteArrayOutputStream();
					/*6*/ TripleHandler handler = new NTriplesWriter(out);
					      try {
					/*7*/     runner.extract(source, handler);
					      } finally {
					/*8*/     handler.close();
					      }
					      	      
					/*9*/ String n3 = out.toString("UTF-8");

		        	outKey.set(new Uri(value.getRecord().getHeader().getTargetURI()).getHost().toLowerCase());
		        	outVal.set(n3);
		        	
		        	context.write(outKey, outVal);
				}
	        }
			
			
	      }
	      catch (Exception ex) {
	        LOG.error("Caught Exception", ex);
	        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
	      }
	}
}
}