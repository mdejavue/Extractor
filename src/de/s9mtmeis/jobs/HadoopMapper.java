package de.s9mtmeis.jobs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractorFactory;
import org.apache.any23.extractor.ExtractorGroup;
import org.apache.any23.extractor.ExtractorRegistryImpl;
import org.apache.any23.extractor.html.HProductExtractorFactory;
import org.apache.any23.extractor.microdata.MicrodataExtractorFactory;
import org.apache.any23.extractor.rdfa.RDFa11ExtractorFactory;
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
		private Text outKey = new Text();
		private Text outVal = new Text();
		
		public final static List<String> EXTRACTORS = 
				Arrays.asList(		"html-rdfa11",
									"html-microdata",
									"html-mf-hproduct" );
		
		public final static List<Pattern> defaultPatterns = 
				Arrays.asList(		Pattern.compile("(property|typeof|about|resource)\\s*="),
									Pattern.compile("(itemscope|itemprop\\s*=)"),
									Pattern.compile("hproduct"));
		
		private List<Pattern> positivePatterns;
		private List<Pattern> negativePatterns;

		private Any23 runner;

		public Extractor() {			
			ExtractorRegistryImpl.getInstance().register(new RDFa11ExtractorFactory());
			ExtractorRegistryImpl.getInstance().register(new MicrodataExtractorFactory());
			ExtractorRegistryImpl.getInstance().register(new HProductExtractorFactory());
			ExtractorGroup extractorGroup = ExtractorRegistryImpl.getInstance().getExtractorGroup(EXTRACTORS);
			
			
			runner = new Any23(extractorGroup);
			LOG.info("Any23 runner has been initialized with extractorGroup");
		}
		
		
		@Override
		public void map(LongWritable key, WARCWritable value, Context context) throws IOException {
			
			if (positivePatterns == null) { // do this only once :)
				positivePatterns = new ArrayList<Pattern>();
				negativePatterns = new ArrayList<Pattern>();
				
				if (context.getConfiguration().get("matchers") != null && context.getConfiguration().get("matchers").length() > 1 ) {
					for (String m : context.getConfiguration().get("matchers").split(" ;; "))
					{
						LOG.info("compiling matcher pattern: " + m);	
						
						if ( m.startsWith("!")) {
							negativePatterns.add(Pattern.compile(m.substring(1)));
						}
						else
						{
							positivePatterns.add(Pattern.compile(m));
						}
					}
				}
			
			try {
				if ( value.getRecord().getHeader().getContentType().equals("application/http; msgtype=response")) {
					//Get the text content as a string.
					byte[] rawData = value.getRecord().getContent();
					String pageText = new String(rawData);
					boolean passedDefaults = false;
					boolean foundPositives = false;
					boolean foundNegatives = false;

					// Increment for LOG
					context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
					
					
					// Check if patterns occur in document
					for (Pattern p : defaultPatterns)
					{
						Matcher m = p.matcher(pageText);
						if (m.find()) {
							passedDefaults = true;
							break;
						}
					}

					if (passedDefaults) {
						for (Pattern p : positivePatterns)
						{
							Matcher m = p.matcher(pageText);
							if (m.find()) {
								foundPositives = true;
								break;
							}
						}
						
						for (Pattern p : negativePatterns)
						{
							Matcher m = p.matcher(pageText);
							if (m.find()) {
								foundNegatives = true;
								break;
							}
						}
					}
					

					if (	passedDefaults 
							&& (foundPositives || positivePatterns.size() < 1) 
							&& (!foundNegatives || negativePatterns.size() < 1)) {    

						/*4*/ DocumentSource source = new StringDocumentSource(pageText, value.getRecord().getHeader().getTargetURI(), value.getRecord().getHeader().getContentType());
						/*5*/ ByteArrayOutputStream out = new ByteArrayOutputStream();
						/*6*/ TripleHandler handler = new ReportingTripleHandler(new IgnoreAccidentalRDFa(new NTriplesWriter(out), true));
						try {
						/*7*/ runner.extract(source, handler);
						} finally {
						/*8*/ handler.close();
						}

						/*9*/ String n3 = out.toString("UTF-8");

						outKey.set("NEW_MAPPER_ENTITY");
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
}