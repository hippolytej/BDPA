package stopwords;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StopWords{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "StopWords");
		job.setJarByClass(StopWords.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
//		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(10);
//		job.setNumReduceTasks(50);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		conf.set("mapreduce.map.output.compress", "true");
//		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

		job.waitForCompletion(true);
	}



	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}'\"()&<>~_-12345677890#$*^%/@\\`=+|");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, ONE);
			}
		}
	}

	// public static class Combiner extends Reducer<Text,IntWritable, Text,IntWritable> {
	// 	@Override
	// 	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	// 		int sum = 0;
	// 		for (IntWritable val : values) {
	// 			sum += val.get();
	// 		}
	// 		context.write(key, new IntWritable(sum));
	// 	}
	// }

	public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if (sum > 4000){
				context.write(key, NullWritable.get());
			}
		}
	}
}
