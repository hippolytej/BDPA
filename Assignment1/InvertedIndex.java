package invertedindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class InvertedIndex{
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
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



	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			FileSplit split = (FileSplit) context.getInputSplit();
			String filename = split.getPath().getName().toString();

			String line = value.toString().toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}\"'()~_-");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, new Text(filename));
			}
		}
	}


	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

//			Check if key is a stop word	-----------------------		
			
//			Test with local file for standalone mode
//			File file = new File("stopwords.csv");
//			Scanner sw = new Scanner(file);
			
//			With DHFS file
            Path pt=new Path("stopwords.csv");
            FileSystem fs = FileSystem.get(new Configuration());
            Scanner sw=new Scanner(fs.open(pt));
			
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
					if (filesFrequency.isEmpty()){
						filesFrequency = fileName + "#" + freq;
					}
					else{
						filesFrequency = filesFrequency + ", "+ fileName + "#" + freq;
					}
				}
				
				Text documentList = new Text();
				documentList.set(filesFrequency.toString());
				context.write(key, documentList);
			}
		}
	}
}
