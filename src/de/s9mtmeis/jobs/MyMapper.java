package de.s9mtmeis.jobs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;
import com.sun.jndi.toolkit.url.Uri;


public class MyMapper {
	

	private static final Logger LOG = Logger.getLogger(MyMapper.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	
public static class GermanUriCounterMapper extends Mapper<LongWritable, WARCWritable, Text, LongWritable> {

	private Text outKey = new Text();
	private LongWritable outVal = new LongWritable(1);
	
	
	@Override
	public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
		try {

	        // Get the text content as a string.
			//byte[] rawData = value.getRecord().getContent();
			//String pageText = new String(rawData);
			
			context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);

	        if ( value.getRecord().getHeader().getContentType().equals("application/http; msgtype=response")
	        		&& ( value.getRecord().getHeader().getTargetURI().contains(".de") 
	        		|| value.getRecord().getHeader().getTargetURI().contains("/de/") ) ) {
	        	
	        	
	        	outKey.set(new Uri(value.getRecord().getHeader().getTargetURI()).getHost().toLowerCase());
	        	
	        	context.write(outKey, outVal);
	        }
			
			
	      }
	      catch (Exception ex) {
	        LOG.error("Caught Exception", ex);
	        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
	      }
	}
}
}