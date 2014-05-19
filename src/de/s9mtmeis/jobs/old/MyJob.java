package de.s9mtmeis.jobs.old;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.martinkl.warc.mapreduce.WARCInputFormat;

public class MyJob extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(MyJob.class);


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		String outputPath = "/tmp/cc/";
		String configFile = null;
		
		// Read the command line arguments.
	    if (args.length <  1)
	      throw new IllegalArgumentException("Example JAR must be passed an output path.");

	    outputPath = args[0];

	    if (args.length >= 2)
	      configFile = args[1];
		
		// Read in any additional config parameters.
	    if (configFile != null) {
	      LOG.info("adding config parameters from '"+ configFile + "'");
	      this.getConf().addResource(configFile);
	    }
		
		Configuration conf = getConf();
		//
		Job job = new Job(conf);
		job.setJarByClass(MyJob.class);
		job.setNumReduceTasks(1);

		String inputPath = "s3n://aws-publicdatasets/common-crawl/crawl-data/CC-MAIN-2014-10/segments/1394010776308/warc/CC-MAIN-20140305091256-00000-ip-10-183-142-35.ec2.internal.warc.gz";
		LOG.info("Input path: " + inputPath);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		FileSystem fs = FileSystem.get(new URI(outputPath), conf);
		
		if (fs.exists(new Path(outputPath)))
		      fs.delete(new Path(outputPath), true);

		    // Set the path where final output 'part' files will be saved.
		    LOG.info("setting output path to '" + outputPath + "'");
		    FileOutputFormat.setOutputPath(job, new Path(outputPath));
		    FileOutputFormat.setCompressOutput(job, false);
		
//		FileSystem fs = FileSystem.newInstance(conf);
//		if (fs.exists(new Path(outputPath))) {
//			fs.delete(new Path(outputPath), true);
//		}
		

		job.setInputFormatClass(WARCInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);

	    job.setMapperClass(MyMapper.GermanUriCounterMapper.class);
	    job.setReducerClass(LongSumReducer.class);

	    return job.waitForCompletion(true) ? 0 : -1;
	}
}
