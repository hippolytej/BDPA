package basicinvertedindex;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BasicInvertedIndex{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(BasicInvertedIndex.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
//		job.setNumReduceTasks(10);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

		job.waitForCompletion(true);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit split = (FileSplit) context.getInputSplit();
			String filename = split.getPath().getName().toString();

			String line = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}'\"()&<>~_-12345677890#$*^%/@\\`=+|");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, new Text(filename));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		String stopwords = new String();
		
		public void setup(Context context) throws IOException, InterruptedException {
//		Check if key is a stop word	-----------------------

//			Test with local file for standalone mode
			File file = new File("stopwords.csv");
			Scanner sw = new Scanner(file);			


//		With DHFS file
//			Path pt=new Path("stopwords.csv");
//	        FileSystem fs = FileSystem.get(new Configuration());
//	        Scanner sw=new Scanner(fs.open(pt));
	        
			while (sw.hasNext()){
				stopwords = stopwords + " " + sw.next().toString();
			}
			sw.close();
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {


			if (!stopwords.contains(key.toString())){
				String filesFrequency = new String();
				for (Text val : values) {
					if (!filesFrequency.contains(val.toString())){
						filesFrequency = filesFrequency + val.toString() + ", ";
					}
				}

				filesFrequency =  filesFrequency.substring(0, filesFrequency.length()-2);
				Text finalIndex = new Text();
				finalIndex.set(filesFrequency);
				context.write(key, finalIndex);
			}
		}
	}
}
