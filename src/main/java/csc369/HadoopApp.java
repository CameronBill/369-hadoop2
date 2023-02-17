package csc369;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

		Job job = new Job(conf, "Hadoop example");
		Job job1 = new Job(conf, "Extra");
		Job job2 = new Job(conf, "Another");
		Job job3 = new Job(conf, "getting complex");

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 3) {
			System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
			System.exit(-1);
		} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

			MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class);
			MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class);

			job.setReducerClass(UserMessages.JoinReducer.class);

			job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(WordCount.ReducerImpl.class);
			job.setMapperClass(WordCount.MapperImpl.class);
			job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
			job.setReducerClass(AccessLog.ReducerImpl.class);
			job.setMapperClass(AccessLog.MapperImpl.class);
			job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		} else if ("RequestsPerCountry".equalsIgnoreCase(otherArgs[0])) {
			File temp = new File("temp_out");
			if (temp.exists()) {
				for (String entry : temp.list()) {
					File tempEntry = new File(temp.getPath(), entry);
					tempEntry.delete();
				}
				temp.delete();
			}
			temp = new File("temp_out1");
			if (temp.exists()) {
				for (String entry : temp.list()) {
					File tempEntry = new File(temp.getPath(), entry);
					tempEntry.delete();
				}
				temp.delete();
			}

			job1.setReducerClass(AccessLog.ReducerImpl.class);
			job1.setMapperClass(AccessLog.MapperImpl.class);
			job1.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
			job1.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
			FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job1, new Path("temp_out"));

			job1.waitForCompletion(true);

			MultipleInputs.addInputPath(job2, new Path("temp_out/part-r-00000"),
					TextInputFormat.class, RequestsPerCountry.RequestMapper.class);
			MultipleInputs.addInputPath(job2, new Path(otherArgs[2]),
					TextInputFormat.class, RequestsPerCountry.CountryMapper.class);

			job2.setReducerClass(RequestsPerCountry.CountryReducer.class);

			job2.setOutputKeyClass(RequestsPerCountry.OUTPUT_KEY_CLASS);
			job2.setOutputValueClass(RequestsPerCountry.OUTPUT_VALUE_CLASS);

			FileOutputFormat.setOutputPath(job2, new Path("temp_out1"));

			job2.waitForCompletion(true);

			job3.setReducerClass(RequestsPerCountry.RequestSummer.class);
			job3.setMapperClass(RequestsPerCountry.RequestCollector.class);
			job3.setOutputKeyClass(RequestsPerCountry.OUTPUT_KEY_CLASS);
			job3.setOutputValueClass(RequestsPerCountry.GROUPING_OUTPUT_VALUE_CLASS);
			FileInputFormat.addInputPath(job3, new Path("temp_out1/part-r-00000"));
			FileOutputFormat.setOutputPath(job3, new Path("temp_out2"));

			job3.waitForCompletion(true);

			job.setMapperClass(RequestsPerCountry.SortMapper.class);
			job.setReducerClass(RequestsPerCountry.SortReducer.class);
			job.setOutputKeyClass(RequestsPerCountry.SORT_OUTPUT_KEY_CLASS);
			job.setOutputValueClass(RequestsPerCountry.OUTPUT_VALUE_CLASS);

			FileInputFormat.addInputPath(job, new Path("temp_out2/part-r-00000"));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		} else {
			System.out.println("Unrecognized job: " + otherArgs[0]);
			System.exit(-1);
		}
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
