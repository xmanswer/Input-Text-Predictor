import java.io.IOException;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class NGram {

	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			String[] wordArray = value.toString().replaceAll("[^a-zA-Z]+", " ")
					.trim().split("\\s+");

			for(int i = 0; i < wordArray.length; i++) {
				StringBuilder words = new StringBuilder();
				for(int j = i; (j - i) < 5 && j < wordArray.length; j++) {
					words.append(wordArray[j].toLowerCase());
					Text text = new Text();
					text.set(words.toString());
					context.write(text, one);
					words.append(" ");
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		Job job = Job.getInstance(conf, "NGram");
		job.setJarByClass(NGram.class);
		job.setMapperClass(NGramMapper.class);
		job.setCombinerClass(NGramReducer.class);
		job.setReducerClass(NGramReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}