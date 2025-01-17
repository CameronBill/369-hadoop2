package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestsPerCountry {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static final Class GROUPING_OUTPUT_VALUE_CLASS = IntWritable.class;

    public static final Class SORT_OUTPUT_KEY_CLASS = IntWritable.class;

    public static class RequestMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split("\t");
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

    public static class CountryReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int numRequests = 0;
            String country = null;
            Iterator<Text> itr = values.iterator();
            String temp = itr.next().toString().strip();
            while (itr.hasNext() == false) {
            }
            for (int i = 0; i < temp.length(); i++) {
                if (!Character.isDigit(temp.charAt(i))) {
                    numRequests = Integer.parseInt(itr.next().toString().strip());
                    country = temp;
                    break;
                }
            }
            if (country == null) {
                numRequests = Integer.parseInt(temp);
                country = itr.next().toString().strip();
            }

            context.write(new Text(country), new IntWritable(numRequests));
        }
    }

    public static class RequestCollector extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split("\t");
            String country = sa[0];
            int numRequests = Integer.parseInt(sa[1]);
            context.write(new Text(country), new IntWritable(numRequests));
        }
    }

    public static class RequestSummer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String sa[] = value.toString().split("\t");
            Text country = new Text(sa[0]);
            IntWritable numRequests = new IntWritable(Integer.parseInt(sa[1]));
            context.write(numRequests, country);
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable wc1,
                WritableComparable wc2) {
            IntWritable int1 = (IntWritable) wc1;
            IntWritable int2 = (IntWritable) wc2;
            return -(int1.compareTo(int2));
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();

            while (itr.hasNext()) {
                context.write(itr.next(), key);
            }
        }
    }

}
