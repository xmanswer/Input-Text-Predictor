import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;


public class LangModel {
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text phrase = new Text();
		private Text word_count = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int t = Integer.parseInt(context.getConfiguration().get("t"));
			String line = value.toString();
			String[] parts = line.split("\t");
			int count = Integer.parseInt(parts[1]);
			if (count > t) {
				String[] words = parts[0].split(" ");
				if (words.length != 1) {
					int indexOfLastSpace = parts[0].lastIndexOf(" ");
					String p = parts[0].substring(0, indexOfLastSpace);
					String w = parts[0].substring(indexOfLastSpace + 1);
					phrase.set(p);
					word_count.set(w + "," + parts[1]);
					context.write(phrase, word_count);
				}
			}
		}
	}

	public static class Reduce extends  TableReducer<Text, Text, NullWritable>  {
		private final byte[] CF = "prob".getBytes();
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int n = Integer.parseInt(context.getConfiguration().get("n"));
			// Read input
			String[] parts;
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			CMP cmp = new CMP();
			int count = 0;
			int temp;
			for (Text val : values) {
				parts = val.toString().split(",");
				temp = Integer.parseInt(parts[1]);
				count += temp;
				map.put(parts[0], temp);
			}
			// Sort
			List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>();
			list.addAll(map.entrySet());
			Collections.sort(list, cmp);
			Collections.reverse(list);
			// Write to HBase
			Put put = new Put(Bytes.toBytes(key.toString()));
			String s;
			Double pr;
			Entry<String, Integer> e;
			int m = Math.min(n, list.size());
			for (int i = 0;i < m; i++) {
				e = list.get(i);
				s = e.getKey();
				pr = (double)e.getValue() / (double)count;
				put.add(CF, Bytes.toBytes(s), Bytes.toBytes(pr.toString()));
			}
			context.write(null, put);
		}
	}
	
	private static class CMP implements Comparator<Entry<String, Integer>> {
		@Override
		public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
			return o1.getValue() - o2.getValue();
		}
	}

	private static final String TABLE_NAME = "lm"; 
	
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			throw new Exception("Wrong number of arguments.");
		}

		Configuration conf = new Configuration();
		conf.set("t", args[2]);
		conf.set("n", args[3]);
		Job job = Job.getInstance(conf, "LangModel");
		TableMapReduceUtil.initTableReducerJob("lm", Reduce.class, job);
		job.setJarByClass(LangModel.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}