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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NewsHash {
	public static class MyMap extends Mapper<Object, Text, LongWritable, IntWritable>{
		private static Text line = new Text();
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long num = 0;
			line = value;
			int length = line.getLength();
			if(length < 6){
				return;
			}else{
				for(int i=0; i<length-5; i++){
					num = (num % (long)Math.pow(26, 5))*26 + line.charAt(i) % 26;
					context.write(new LongWritable(num), new IntWritable(1));
				}
			}
		}
	}
	
	public static class MyReduce extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>{
		
		@Override
		protected void reduce(LongWritable key, Iterable<IntWritable> iterator,Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable intWritable : iterator) {
				sum+=intWritable.get();
			}
			context.write(key, new IntWritable(sum));
		}
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "NewsHash");
		job.setJarByClass(NewsHash.class);
		job.setMapperClass(MyMap.class);
		job.setCombinerClass(MyReduce.class);
		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(IntWritable.class);	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/input/a"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output4"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
