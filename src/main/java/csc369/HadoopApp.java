package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("Query1".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query1.ReducerImpl.class);
		job.setMapperClass(Query1.MapperImpl.class);
		job.setOutputKeyClass(Query1.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query1.OUTPUT_VALUE_CLASS);
	} else if ("Query1Sort".equalsIgnoreCase(otherArgs[0])) {
		// job.setReducerClass(Query1Sort.ReducerImpl.class);
		job.setMapperClass(Query1Sort.MapperImpl.class);
		job.setOutputKeyClass(Query1Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query1Sort.OUTPUT_VALUE_CLASS);
	} else if ("Query2".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query2.ReducerImpl.class);
		job.setMapperClass(Query2.MapperImpl.class);
		job.setOutputKeyClass(Query2.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query2.OUTPUT_VALUE_CLASS);
	} else if ("Query3".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query3.ReducerImpl.class);
		job.setMapperClass(Query3.MapperImpl.class);
		job.setOutputKeyClass(Query3.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query3.OUTPUT_VALUE_CLASS);
	} else if ("Query4".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(Query4.ReducerImpl.class);
	    job.setMapperClass(Query4.MapperImpl.class);
	    job.setOutputKeyClass(Query4.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Query4.OUTPUT_VALUE_CLASS);
	} else if ("Query4Sort".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(Query4Sort.MapperImpl.class);
		job.setOutputKeyClass(Query4Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query4Sort.OUTPUT_VALUE_CLASS);
	} else if ("Query5".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query5.ReducerImpl.class);
		job.setMapperClass(Query5.MapperImpl.class);
		job.setOutputKeyClass(Query5.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query5.OUTPUT_VALUE_CLASS);
	} else if ("Query5Sort".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query5Sort.ReducerImpl.class);
		job.setMapperClass(Query5Sort.MapperImpl.class);
		job.setOutputKeyClass(Query5Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query5Sort.OUTPUT_VALUE_CLASS);
	} else if ("Query6".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query6.ReducerImpl.class);
		job.setMapperClass(Query6.MapperImpl.class);
		job.setOutputKeyClass(Query6.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query6.OUTPUT_VALUE_CLASS);
	} else if ("Query6Sort".equalsIgnoreCase(otherArgs[0])) {
		job.setMapperClass(Query6Sort.MapperImpl.class);
		job.setOutputKeyClass(Query6Sort.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query6Sort.OUTPUT_VALUE_CLASS);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
