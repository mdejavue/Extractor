package de.s9mtmeis.jobs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.martinkl.warc.mapreduce.WARCInputFormat;


/**
 * Runs the Hadoop Job of the data extract
 * 
 * @author Steffen Stadtm�ller <steffen.stadtmueller@kit.edu>
 */
public class RDFExtractJob extends Configured implements Tool {
  /**
   * Contains the Amazon S3 bucket holding the CommonCrawl corpus.
   */
  private static final String CC_BUCKET = "commoncrawl-crawl-002";


  public static void main(String[] args) throws Exception {
	  int res = ToolRunner.run(new Configuration(), new RDFExtractJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-10/segments/1394010776308/warc/CC-MAIN-20140305091256-00000-ip-10-183-142-35.ec2.internal.warc.gz";
	  	String outputPath = "/tmp/cc/";
		String configFile = null;
		Configuration conf = new Configuration();
		
		// Read the command line arguments.
	    if (args.length <  1)
	      throw new IllegalArgumentException("Example JAR must be passed an output path.");

	    outputPath = args[0];

	    if (args.length >= 2)
	      configFile = args[1];
		
		// Read in any additional config parameters.
	    if (configFile != null) {
	      //LOG.info("adding config parameters from '"+ configFile + "'");
	      conf.addResource(configFile);
	    }

		//
		Job job = new Job(conf);
	    job.setJarByClass(RDFExtractJob.class);
		//job.setNumReduceTasks(1);


		//LOG.info("Input path: " + inputPath);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		FileSystem fs = FileSystem.get(new URI(outputPath), conf);
		
		if (fs.exists(new Path(outputPath)))
		      fs.delete(new Path(outputPath), true);

	    // Set the path where final output 'part' files will be saved.
	    //LOG.info("setting output path to '" + outputPath + "'");
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    FileOutputFormat.setCompressOutput(job, false);
		
		job.setInputFormatClass(WARCInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(RDFExtractMapper.RDFExtractMapperImpl.class);
	    job.setReducerClass(RDFExtractReducer.class);

	    // Allows some (5%) of tasks fail; we might encounter the 
	    // occasional troublesome set of records and skipping a few 
	    // of 1000s won't hurt counts too much.
	    conf.set("mapred.max.map.failures.percent", "5");
	    
	    return job.waitForCompletion(true) ? 0 : -1;
  }
}
