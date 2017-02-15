package invertedindex;

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
//		job.setNumReduceTasks(10);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		conf.set("mapreduce.map.output.compress", "true");
//		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
	
		
		job.waitForCompletion(true);
		Counter counter = job.getCounters().findCounter(CustomCounters.UNIQUEWORDS);
		System.out.println("Unique words in a single file counter = " + counter.getValue());
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
			long s = 0;
			long t = 0;
			long a = 0;
			for (Text val : values) {
				
				if (val.toString().equals("pg100.txt")){
					s=s+1;
				}
				else if (val.toString().equals("pg31100.txt")){
					t=t+1;
				}
				else if (val.toString().equals("pg3200.txt")){
					a=a+1;
				}
			}
			String filesFrequency = new String();
			if (s>0){
				filesFrequency = filesFrequency + "pg100.txt#" + String.valueOf(s)+",";
			}
			if (t>0){
				filesFrequency = filesFrequency + "pg31100.txt#" + String.valueOf(t)+",";
			}
			if (a>0){
				filesFrequency = filesFrequency + "pg3200.txt#" + String.valueOf(a)+",";
			}
			Text documentList = new Text();
			documentList.set(filesFrequency.toString());
			context.write(key, documentList);
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

//			Check if key is a stop word	-----------------------		
			
//			Test with local file for standalone mode
			File file = new File("stopwords.csv");
			Scanner sw = new Scanner(file);
			
//			With DHFS file
//            Path pt=new Path("stopwords.csv");
//            FileSystem fs = FileSystem.get(new Configuration());
//            Scanner sw=new Scanner(fs.open(pt));
			
			Boolean isSW = false;
			while (sw.hasNext()){
				if (sw.next().toString().equals(key.toString())) {
					isSW = true;
					break;
				}
			}
			sw.close();
			
//			Create index and count frequencies ---------------
			
			if (!isSW){
				
				
				
				long fs = 0;
				long ft = 0;
				long fa = 0;
				for (Text val : values) {
			        for (String token: val.toString().split(",")) {
			        	String[] couple = token.split("#");
						if (couple[0].equals("pg100.txt")){
							fs = fs + Long.valueOf(couple[1]).longValue();
						}
						else if (couple[0].equals("pg31100.txt")){
							ft = ft + Long.valueOf(couple[1]).longValue();
						}
						else if (couple[0].equals("pg3200.txt")){
							fa = fa + Long.valueOf(couple[1]).longValue();
						}
					}
				}
				
				String finalFilesFrequency = new String();
				if (fs>0){
					finalFilesFrequency = finalFilesFrequency + "pg100.txt#" + String.valueOf(fs)+", ";
				}
				if (ft>0){
					finalFilesFrequency = finalFilesFrequency + "pg31100.txt#" + String.valueOf(ft)+", ";
				}
				if (fa>0){
					finalFilesFrequency = finalFilesFrequency + "pg3200.txt#" + String.valueOf(fa)+", ";
				}
				

				finalFilesFrequency =  finalFilesFrequency.substring(0, finalFilesFrequency.length()-2);

				
//				Here comes the counter...
				if (!finalFilesFrequency.contains(",")){
					context.getCounter(CustomCounters.UNIQUEWORDS).increment(1);
				}
				
//				Now back to the good part
				
				Text documentList = new Text();
				documentList.set(finalFilesFrequency);
				context.write(key, documentList);
			}
		}
	}
}
