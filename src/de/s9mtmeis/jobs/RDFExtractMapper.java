package de.s9mtmeis.jobs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.martinkl.warc.WARCWritable;



public class RDFExtractMapper {


	private static final Logger LOG = Logger.getLogger(RDFExtractMapper.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	public static class RDFExtractMapperImpl extends Mapper<LongWritable, WARCWritable, Text, Text> {

		@Override
		public void map(LongWritable key, WARCWritable value, Context context) throws IOException {


			try {

				RDFExtractor extractor = new RDFExtractor(context, new Text(value.getRecord().getHeader().getRecordID()), true);
				if (extractor.supports(value.getRecord().getHeader().getContentType())) {

					// parse item and return stats
					//Map<String, Object> stats = extractor.extract(value);
					StringBuilder sb = extractor.extract(value);

					if (!sb.toString().equals("hadExtractionError")) {
						// data about the file where this data came from
						sb.append(value.getRecord().getHeader().getRecordID()+",");
						sb.append(value.getRecord().getHeader().getContentLength()+",");

						// data about the web page crawled
						sb.append(value.getRecord().getHeader().getTargetURI()+",");
						sb.append(value.getRecord().getHeader().getDateString()+",");
						sb.append(value.getRecord().getHeader().getContentType()+",");

						context.write(new Text("stats4"+value.getRecord().getHeader().getTargetURI()), new Text(sb.toString()));
					}
				}
			}
			catch (Exception e) {
				LOG.error("Caught Exception", e);
				context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
			}
		}	
	}
}