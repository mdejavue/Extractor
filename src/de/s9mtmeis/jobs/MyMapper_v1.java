package de.s9mtmeis.jobs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;


public class MyMapper_v1 {
	

	private static final Logger LOG = Logger.getLogger(MyMapper_v1.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	
public static class TagCounterMapper extends Mapper<LongWritable, WARCWritable, Text, LongWritable> {

	
	private Text outKey = new Text();
	private LongWritable outVal = new LongWritable(1);
	// The HTML regular expression is case insensitive (?i), avoids closing tags (?!/),
	// tries to find just the tag name before any spaces, and then consumes any other attributes.
	private static final String HTML_TAG_PATTERN = "(?i)<(?!/)([^\\s>]+)([^>]*)>";
	private Pattern patternTag;
	private Matcher matcherTag;
	
	
	
	@Override
	public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
		patternTag = Pattern.compile(HTML_TAG_PATTERN);

		try {
		LOG.debug(value.getRecord().getHeader().getTargetURI() + " -- " + value.getRecord().getHeader().getContentLength());
			
		String recordType = value.getRecord().getHeader().getRecordType();
		String targetURL  = value.getRecord().getHeader().getTargetURI();

		
		if (recordType.equals("response") && targetURL != null) {
		    

			// Convenience function that reads the full message into a raw byte array
			byte[] rawData = value.getRecord().getContent();
			String content = new String(rawData);
			// The HTTP header gives us valuable information about what was received during the request
			String headerText = content.substring(0, content.indexOf("\r\n\r\n"));

			// In our task, we're only interested in text/html, so we can be a little lax
			// TODO: Proper HTTP header parsing + don't trust headers
			if (headerText.contains("Content-Type: text/html")) {
				context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
				// Only extract the body of the HTTP response when necessary
				// Due to the way strings work in Java, we don't use any more memory than before
				String body = content.substring(content.indexOf("\r\n\r\n") + 4);
				// Process all the matched HTML tags found in the body of the document
				matcherTag = patternTag.matcher(body);
				while (matcherTag.find()) {
					String tagName = matcherTag.group(1);
					outKey.set(tagName.toLowerCase());
					context.write(outKey, outVal);
				}
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