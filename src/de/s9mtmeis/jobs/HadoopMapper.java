package de.s9mtmeis.jobs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractorFactory;
import org.apache.any23.extractor.ExtractorGroup;
import org.apache.any23.extractor.ExtractorRegistryImpl;
import org.apache.any23.extractor.html.AdrExtractorFactory;
import org.apache.any23.extractor.html.GeoExtractorFactory;
import org.apache.any23.extractor.html.HCalendarExtractorFactory;
import org.apache.any23.extractor.html.HCardExtractorFactory;
import org.apache.any23.extractor.html.HListingExtractorFactory;
import org.apache.any23.extractor.html.HProductExtractorFactory;
import org.apache.any23.extractor.html.HRecipeExtractorFactory;
import org.apache.any23.extractor.html.HResumeExtractorFactory;
import org.apache.any23.extractor.html.HReviewExtractorFactory;
import org.apache.any23.extractor.microdata.MicrodataExtractorFactory;
import org.apache.any23.extractor.rdfa.RDFa11ExtractorFactory;
import org.apache.any23.extractor.rdfa.RDFaExtractorFactory;
import org.apache.any23.filter.IgnoreAccidentalRDFa;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.source.StringDocumentSource;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.ReportingTripleHandler;
import org.apache.any23.writer.TripleHandler;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;


public class HadoopMapper {


	private static final Logger LOG = Logger.getLogger(HadoopJob.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}

	public static class Extractor extends Mapper<LongWritable, WARCWritable, Text, Text> {
		
		private Any23 runner;
		private Text OUT_KEY = new Text();
		private Text OUT_VAL = new Text();

		
		public final static Map<String, Class<?>> AVAILABLE_EXTRACTORS;
		static {
	        Map<String, Class<?>> aMap = new HashMap<String, Class<?>>();
	        aMap.put("html-rdfa", RDFaExtractorFactory.class);
	        aMap.put("html-rdfa11", RDFa11ExtractorFactory.class);
	        aMap.put("html-microdata", MicrodataExtractorFactory.class);
	        aMap.put("html-mf-adr", AdrExtractorFactory.class);
	        aMap.put("html-mf-geo", GeoExtractorFactory.class);
	        aMap.put("html-mf-hcalendar", HCalendarExtractorFactory.class);
	        aMap.put("html-mf-hcard", HCardExtractorFactory.class);
	        aMap.put("html-mf-hlisting", HListingExtractorFactory.class);
	        aMap.put("html-mf-hrecipe", HRecipeExtractorFactory.class);
	        aMap.put("html-mf-hresume", HResumeExtractorFactory.class);
	        aMap.put("html-mf-hreview", HReviewExtractorFactory.class);
	        aMap.put("html-mf-hproduct", HProductExtractorFactory.class);
	        AVAILABLE_EXTRACTORS = Collections.unmodifiableMap(aMap);
	    }
		
		private static ArrayList<String> EXTRACTORS = new ArrayList<String>();
		private static ArrayList<Pattern> POSITIVE_PATTERNS = new ArrayList<Pattern>();
		private static ArrayList<Pattern> NEGATIVE_PATTERNS = new ArrayList<Pattern>();

		
		private void registerExtractors(List<String> EXTRACTORS) {
			
			for ( String descriptor : EXTRACTORS) {
				try {
					ExtractorRegistryImpl.getInstance().register( (ExtractorFactory<?>)AVAILABLE_EXTRACTORS.get(descriptor).newInstance() );
				}
				catch (Exception failed) {
					continue;
				}
			}
		}
		

		private void configureExtraction(Context context) {
			
			String extractorParms = context.getConfiguration().get("extractors");
			String matcherParms = context.getConfiguration().get("matchers");			
			
			if (extractorParms != null && extractorParms.length() > 1 ) {
				for (String extractor : extractorParms.split(" ;; "))
				{
					LOG.info("adding extractor to list: " + extractor);	
					EXTRACTORS.add(extractor);
				}
			}
			
			if (matcherParms != null && matcherParms.length() > 1 ) {
				for (String matcher : matcherParms.split(" ;; "))
				{
					LOG.info("compiling matcher pattern: " + matcher);					
					if ( matcher.startsWith("!")) {
						NEGATIVE_PATTERNS.add(Pattern.compile(matcher.substring(1)));
					}
					else
					{
						POSITIVE_PATTERNS.add(Pattern.compile(matcher));
					}
				}
			}
			
			registerExtractors(EXTRACTORS);
			ExtractorGroup extractorGroup = ExtractorRegistryImpl.getInstance().getExtractorGroup(EXTRACTORS);
			runner = new Any23(extractorGroup);
			LOG.info("Any23 runner has been initialized with extractorGroup");	
		}
		
		
		@Override
		public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
			
			if (EXTRACTORS.isEmpty()) {
				configureExtraction(context); // do this only once :)
			}
				
			try {
				if ( value.getRecord().getHeader().getContentType().equals("application/http; msgtype=response")) {
					//Get the text content as a string.
					byte[] rawData = value.getRecord().getContent();
					String headerText = new String(value.getRecord().getHeader().toString());
					String pageText = new String(rawData);
					String matcherText = headerText + pageText;
					
					boolean foundPositives = true;
					boolean foundNegatives = false;

					// Increment for LOG
					context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);											
					
					for (Pattern p : POSITIVE_PATTERNS)
					{
						Matcher m = p.matcher(matcherText);
						if (m.find()) {
							break;
						}
						foundPositives = false;
					}
					
					for (Pattern p : NEGATIVE_PATTERNS)
					{
						Matcher m = p.matcher(matcherText);
						if (m.find()) {
							foundNegatives = true;
							break;
						}
					}

					if ( foundPositives && !foundNegatives) {    

						/*4*/ DocumentSource source = new StringDocumentSource(pageText, value.getRecord().getHeader().getTargetURI(), value.getRecord().getHeader().getContentType());
						/*5*/ ByteArrayOutputStream out = new ByteArrayOutputStream();
						/*6*/ TripleHandler handler = new ReportingTripleHandler(new IgnoreAccidentalRDFa(new NTriplesWriter(out), true));
						try {
						/*7*/ runner.extract(source, handler);
						} finally {
						/*8*/ handler.close();
						}

						/*9*/ String n3 = out.toString("UTF-8");

						OUT_KEY.set("NEW_MAPPER_ENTITY" 
										+ "::" + value.getRecord().getHeader().getTargetURI()
										+ "::" + value.getRecord().getHeader().getDateString()
										+ "\n");
						OUT_VAL.set(n3);

						context.write(OUT_KEY, OUT_VAL);
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