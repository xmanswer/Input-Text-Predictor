import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LangModel {
	public static class LangModelMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int t = conf.getInt("threshold", 2);
			String[] keyValue = value.toString().split("\t");
			String sentence = keyValue[0];
			int count = Integer.parseInt(keyValue[1]);
			String[] wordArray = sentence.split(" ");
			if(count <= t || wordArray.length < 2) return;
			
			String word = wordArray[wordArray.length-1];
			String phrase = sentence.substring(0, sentence.lastIndexOf(" ")).trim();
			
			word = word + "," + keyValue[1];
			context.write(new Text(phrase), new Text(word));
		}
	}

	public static class LangModelReducer extends TableReducer<Text,Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int n = conf.getInt("topNumber", 5);
			
			Map<String, Integer> map = new HashMap<String, Integer>();
			int totalCount = 0;
					
			for(Text value : values) {
				String[] wordAndCount = value.toString().split(",");
				String word = wordAndCount[0].trim();
				int count = Integer.parseInt(wordAndCount[1].trim());
				totalCount += count;
				map.put(word, count);
			}
			
			ArrayList<Map.Entry<String, Integer>> sortedList 
				= new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
			Collections.sort(sortedList, new CompareCountAlpha());
			
			Put putToHBase = new Put(Bytes.toBytes(key.toString()));
			
			int number = 0;
			for(Map.Entry<String, Integer> entry : sortedList) {
				if(number >= n) break;
				Double probablity = (double)entry.getValue() / (double) totalCount;
				putToHBase.add(Bytes.toBytes("prob"), 
						Bytes.toBytes(entry.getKey()), Bytes.toBytes(probablity.toString()));
				number++;
			}
			context.write(key, putToHBase);
		}
		
		private class CompareCountAlpha implements Comparator<Map.Entry<String, Integer>> {
			 @Override
		        public int compare(Map.Entry<String, Integer> entry1, 
		        		Map.Entry<String, Integer> entry2) {
		            if(entry1.getValue() > entry2.getValue()) return -1;
		            else if(entry1.getValue() < entry2.getValue()) return 1;
		            else {
		            	return entry1.getKey().compareTo(entry2.getKey());
		            }
		        }
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		
		conf.set("threshold", remainingArgs[2]);
		conf.set("topNumber", remainingArgs[3]);
		Job job = Job.getInstance(conf, "LangModel");
		TableMapReduceUtil.initTableReducerJob("lm", LangModelReducer.class, job);
		job.setJarByClass(LangModel.class);
		job.setMapperClass(LangModelMapper.class);
		job.setReducerClass(LangModelReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
