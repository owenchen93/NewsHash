import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Signature {
///map function choose
//x+1
//3x+1
//5x+1
//7x+1
//11x+1
//13x+1
//17x+1
	public static class SignMap extends Mapper<Object, Text, Text, LongWritable>{
		private static Text line = new Text();
		@Override
		protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			line = value;
			String[] items = line.toString().split("\t");
			long num = Long.parseLong(items[0]);
			long total = (long)Math.pow(26, 6);
			long x1 = (num + 1) % total;
			long x2 = (3 * num + 1) % total;
			long x3 = (5 * num + 1) % total;
			long x4 = (7 * num + 1) % total;
			long x5 = (11 * num + 1) % total;
			long x6 = (13 * num + 1) % total;
			long x7 = (17 * num + 1) % total;
			
			context.write(new Text("x+1"), new LongWritable(x1));
			context.write(new Text("3x+1"), new LongWritable(x2));
			context.write(new Text("5x+1"), new LongWritable(x3));
			context.write(new Text("7x+1"), new LongWritable(x4));
			context.write(new Text("11x+1"), new LongWritable(x5));
			context.write(new Text("13x+1"), new LongWritable(x6));
			context.write(new Text("17x+1"), new LongWritable(x7));
		}
	}
	
	public static class SignReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
		@Override
		protected void reduce(Text key, Iterable<LongWritable> iterator,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long min = Long.MAX_VALUE;
			for (LongWritable longWritable : iterator) {
				if(longWritable.get() < min){
					min = longWritable.get();
				}
			}
			context.write(key, new LongWritable(min));
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Signature");
		job.setJarByClass(Signature.class);
		job.setMapperClass(SignMap.class);
		job.setCombinerClass(SignReduce.class);
		job.setReducerClass(SignReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);	
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/output4/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/output41"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
