package de.s9mtmeis.jobs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;


public class HtmlWordsCounterMapper {
	

	private static final Logger LOG = Logger.getLogger(HtmlWordsCounterMapper.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	
public static class WordCounterMapper extends Mapper<LongWritable, WARCWritable, Text, LongWritable> {

	
	@Override
	public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
		try {

	        // Get the text content as a string.
			byte[] rawData = value.getRecord().getContent();
			String pageText = new String(rawData);
			
			context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);

	        // Removes all punctuation.
	        pageText = pageText.replaceAll("[^a-zA-Z0-9 ]", "");

	        // Normalizes whitespace to single spaces.
	        pageText = pageText.replaceAll("\\s+", " ");

	        if (pageText == null || pageText == "") {
	          //
	        }

	        // Splits by space and outputs to OutputCollector.
	        for (String word : pageText.split(" ")) {
	          context.write(new Text(word.toLowerCase()), new LongWritable(1));
	        }
	      }
	      catch (Exception ex) {
	        LOG.error("Caught Exception", ex);
	        context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
	      }
	}
}
}