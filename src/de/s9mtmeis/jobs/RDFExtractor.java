package de.s9mtmeis.jobs;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.any23.Any23;
import org.apache.any23.ExtractionReport;
import org.apache.any23.extractor.ExtractionParameters;
import org.apache.any23.extractor.ExtractorGroup;
import org.apache.any23.extractor.ExtractorRegistryImpl;
import org.apache.any23.mime.MIMEType;
import org.apache.any23.source.ByteArrayDocumentSource;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.vocab.SINDICE;
import org.apache.any23.vocab.XHTML;
import org.apache.any23.writer.RDFXMLWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;


/**
 * Wraps a Any23 instance, defines a specific set of extractors, a list of
 * property namespaces to be filtered. It extracts RDF, collects several
 * statistics on the extraction process, and writes triples to files. For added
 * performance, regex guessers are used to find out whether the document
 * contains data for the specific formats at all.
 * 
 * @author Hannes Muehleisen (hannes@muehleisen.org)
 */
public class RDFExtractor {

	public final static List<String> EXTRACTORS = Arrays.asList("html-rdfa",
			"html-microdata");

	public final static Map<String, Pattern> dataGuessers = new HashMap<String, Pattern>();
	TaskInputOutputContext<LongWritable, WARCWritable, Text, Text> context;
	Text key;

	static {
		Map<String, String> guessers = new HashMap<String, String>();

		// TODO: check performance penalty of * operators and possible
		// workarounds

		guessers.put("html-rdfa", "(property|vocab)\\s*=\"http://schema.org");
		guessers.put("html-microdata", "(itemscope|itemprop\\s*=)");

		guessers.put("html-mf-product", "hproduct");

		for (Map.Entry<String, String> guesser : guessers.entrySet()) {
			dataGuessers.put(guesser.getKey(),
					Pattern.compile(guesser.getValue()));
		}
	}

	public final static List<String> evilNamespaces = Arrays.asList(XHTML.NS,
			SINDICE.NS);

	private static Logger log = Logger.getLogger(RDFExtractor.class);

	private Any23 any23Parser;
	private ExtractorGroup any23extractorGroup;
	ExtractionParameters any23ExParams;
	boolean useGuessers;

	public RDFExtractor(TaskInputOutputContext<LongWritable, WARCWritable, Text, Text> context, Text key, boolean useGuessers)
			throws FileNotFoundException {
		this.context=context;
		this.key=key;
		
		any23extractorGroup = ExtractorRegistryImpl.getInstance()
				.getExtractorGroup(EXTRACTORS);

		any23ExParams = ExtractionParameters.newDefault();
		any23ExParams.setFlag("any23.extraction.metadata.timesize", false);
		any23ExParams.setFlag("any23.extraction.head.meta", false);

		any23Parser = new Any23(any23extractorGroup);
		this.useGuessers = useGuessers;
	}

	public StringBuilder extract(WARCWritable item) {
		//Map<String, Object> extractionStats = new HashMap<String, Object>();
		StringBuilder sb = new StringBuilder();

		try {
			String documentContent = new String(item.getRecord().getContent(), "UTF-8");

			if (this.useGuessers) {
				// check if document contains any structured data
				// possible extension: create ad-hoc extractor list from matches
				boolean foundSth = false;
				for (Map.Entry<String, Pattern> guesser : dataGuessers
						.entrySet()) {
					Matcher m = guesser.getValue().matcher(documentContent);
					if (m.find()) {
						foundSth = true;
						break;
					}
				}

				if (!foundSth) {
					sb.append("hadExtractionError");
					//extractionStats.put("hadExtractionError", Boolean.TRUE);
					return sb;
				}
			}

			DocumentSource any23Source = new ByteArrayDocumentSource(
					documentContent.getBytes(), item.getRecord().getHeader().getTargetURI(),
					item.getRecord().getHeader().getContentType());

			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			RDFXMLWriter writer = new RDFXMLWriter(out);

			
			/**
			 * Call extractor
			 * */
			ExtractionReport report = any23Parser.extract(any23ExParams,
					any23Source, writer);
			Text outVal = new Text();
			outVal.set(out.toString("UTF-8"));
			context.write(key, outVal);
			
			sb.append(report.getDetectedMimeType()+ ",");
			//sb.append(writer.getTotalTriplesFound()+ ",");
			//sb.append(writer.getTriplesPerExtractor()+",");
			
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Unable to parse " + item.getRecord().getHeader().getTargetURI(), e);
			}
			return new StringBuilder("hadExtractionError");
		}
		return sb;
	}

	public boolean supports(String mimeType) {
		MIMEType type = MIMEType.parse(mimeType);
		return !any23extractorGroup.filterByMIMEType(type).isEmpty();
	}
}
