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
import org.apache.log4j.Logger;

import com.martinkl.warc.mapreduce.WARCInputFormat;

public class HadoopJob extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(HadoopJob.class);


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HadoopJob(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		
		// Read the command line arguments.
	    if (args.length <  1)
	      throw new IllegalArgumentException("Insufficient arguments. Read the documentation for all options.");
	    else
	    {
	      LOG.info("adding config parameters from command line:");
	      for ( String s : args)
	      {
	    	  String[] split = s.split("=", 2);
	    	  LOG.info("adding key " + split[0] + " with value " + split[1]);
	    	  this.getConf().set(split[0], split[1]);
	      }
	    }
		
		Job job = new Job(getConf());
		job.setJarByClass(HadoopJob.class);
		job.setNumReduceTasks(0);

		String inputPath = getConf().get("inputPath");
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		String outputPath = getConf().get("outputPath");
		FileSystem fs = FileSystem.get(new URI(outputPath), getConf());
		
		if (fs.exists(new Path(outputPath)))
		      fs.delete(new Path(outputPath), true);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		FileOutputFormat.setCompressOutput(job, false);
		
		job.setInputFormatClass(WARCInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setMapperClass(HadoopMapper.Extractor.class);
	    //job.setReducerClass(ReducerConcatRDFa.class);

	    return job.waitForCompletion(true) ? 0 : -1;
	}
}
