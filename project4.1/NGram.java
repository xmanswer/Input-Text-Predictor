import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGram {
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static int MAX_N = 5;
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().toLowerCase()
					.replaceAll("[^a-zA-Z]+", " ");
			Queue<String> q = new LinkedList<String>();
			StringTokenizer tokenizer = new StringTokenizer(line);
			StringBuffer sb = new StringBuffer();
			Text phrase = new Text();
			while (tokenizer.hasMoreTokens()) {
				if (q.size() != MAX_N) {
					q.add(tokenizer.nextToken());
				} else {
					for (String s : q) {
						sb.append(s);
						sb.append(" ");
						phrase.set(sb.substring(0, sb.lastIndexOf(" ")));
						context.write(phrase, one);
					}
					sb.delete(0, sb.length());
					q.remove();
					q.add(tokenizer.nextToken());
				}
			}
			while (!q.isEmpty()) {
				for (String s : q) {
					sb.append(s);
					sb.append(" ");
					phrase.set(sb.substring(0, sb.lastIndexOf(" ")));
					context.write(phrase, one);
				}
				sb.delete(0, sb.length());
				q.remove();
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "NGram");
		job.setJarByClass(NGram.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}