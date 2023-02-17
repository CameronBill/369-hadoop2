package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestsPerCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class RequestMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");
            String hostname = sa[0];
            String numRequests = sa[1];
            context.write(new Text(hostname), new Text(numRequests));
        }
    }

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split(",");
            String hostname = sa[0];
            String country = sa[1];
            context.write(new Text(hostname), new Text(country));
        }
    }

    public static class CombinerImpl extends Reducer<Text, Text, Text, intWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numRequests;
            String country;
            if (values[0].isdigit()) {
                numRequests = Integer.parseInt(values[0]);
                country = values[1];
            } else {
                numRequests = Integer.parseInt(values[1]);
                country = values[0];
            }

            context.write(new Text(country), new IntWritable(numRequests));
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = values.iterator();

            while (itr.hasNext()) {
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split(",");
            String country = sa[0];
            IntWritable numRequests = new IntWritable(Integer.parseInt(sa[1]));
            context.write(numRequests, country);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();

            while (itr.hasNext()) {
                context.write(itr.next().get(), key);
            }
        }
    }

}
