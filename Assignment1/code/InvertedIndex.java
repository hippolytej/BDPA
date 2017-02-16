package invertedindex;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
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
import org.apache.hadoop.util.Progressable;

public class InvertedIndex{
	static enum CustomCounters {UNIQUEWORDS}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(10);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");


		job.waitForCompletion(true);
		Counter counter = job.getCounters().findCounter(CustomCounters.UNIQUEWORDS);

		FileSystem hdfs = FileSystem.get(URI.create("count"), conf);
		Path file = new Path("counter.txt");
		if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
		OutputStream os = hdfs.create(file);
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		br.write("Unique words in a single file = " + counter.getValue());
		br.close();
		hdfs.close();

		System.out.println("Unique words in a single file = " + counter.getValue());
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

	public static class Combiner extends Reducer<Text,Text, Text,Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> countDooku = new HashMap<String, Integer>();

			for (Text val : values) {
				if(countDooku.containsKey(val.toString())){
					countDooku.put(val.toString(), countDooku.get(val.toString()) + 1);
				}else{
					countDooku.put(val.toString(), 1);
				}
			}

			String filesFrequency = new String();
			for (String fileName: countDooku.keySet()){
				String freq = countDooku.get(fileName).toString();
				filesFrequency = filesFrequency + fileName + "#" + freq + ",";
			}

			filesFrequency =  filesFrequency.substring(0, filesFrequency.length()-1);

			Text index = new Text();
			index.set(filesFrequency.toString());
			context.write(key, index);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		String stopwords = new String();

		public void setup(Context context) throws IOException, InterruptedException {
//		Check if key is a stop word	-----------------------

//			Test with local file for standalone mode
//			File file = new File("stopwords.csv");
//			Scanner sw = new Scanner(file);


//		With DHFS file
			Path pt=new Path("stopwords.csv");
	        FileSystem fs = FileSystem.get(new Configuration());
	        Scanner sw=new Scanner(fs.open(pt));

			while (sw.hasNext()){
				stopwords = stopwords + " " + sw.next().toString();
			}
			sw.close();


		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {


//			Create index and count frequencies ---------------

			if (!stopwords.contains(key.toString())){

				HashMap<String, Integer> countDooku = new HashMap<String, Integer>();

				for (Text val : values) {
					for (String token: val.toString().split(",")) {
						String[] couple = token.split("#");
						if(countDooku.containsKey(couple[0])){
							countDooku.put(couple[0], countDooku.get(couple[0]) + Integer.parseInt(couple[1]));
						}
						else{
							countDooku.put(couple[0], Integer.parseInt(couple[1]));
						}
					}
				}

				String finalFilesFrequency = new String();
				for (String fileName: countDooku.keySet()){
					String freq = countDooku.get(fileName).toString();
					finalFilesFrequency = finalFilesFrequency + fileName + "#" + freq + ", ";
				}

				finalFilesFrequency =  finalFilesFrequency.substring(0, finalFilesFrequency.length()-2);

				Text finalIndex = new Text();
				finalIndex.set(finalFilesFrequency);
				context.write(key, finalIndex);

//				Here comes the counter...
				if (!finalFilesFrequency.contains(",")){
					context.getCounter(CustomCounters.UNIQUEWORDS).increment(1);
				}
			}
		}
	}
}
