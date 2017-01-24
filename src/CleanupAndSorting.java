import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CleanupAndSorting {

	// input: title, normalizedRank==>linkPageTitle1++linkPageTitle2++...
	// output: normalizedRank, title
	// the reason why take normalizedRank as key is I can only find out method
	// to make key comparison in hadoop
	public static class SortMap extends MapReduceBase implements
			Mapper<Text, Text, DoubleWritable, Text> {

		public void map(Text key, Text value,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer st = new StringTokenizer(value.toString(), "==>");
			double rankValue = Double.parseDouble(st.nextToken().trim()
					.toString());

			DoubleWritable rank = new DoubleWritable();
			rank.set(rankValue);
			output.collect(rank, key);
		}
	}

	// trivial
	public static class SortReduce extends MapReduceBase implements
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		public void reduce(DoubleWritable key, Iterator<Text> values,
				OutputCollector<DoubleWritable, Text> output, Reporter reporter)
				throws IOException {

			Text title = new Text();
			// maybe more than 1 pages with the same rank value
			while (values.hasNext()) {
				title = values.next();
				// output.collect(title, key);
				output.collect(key, title);
			}
		}
	}

	public static void main(String[] args) throws IOException {

		JobConf job = new JobConf(CleanupAndSorting.class);
		job.setJobName("Sort");
		job.setMapperClass(SortMap.class);
		job.setReducerClass(SortReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		String finalNormalizedRankOutputPathString = "user/hduser/output/PA2output/Iteration/NormalizedRank"
				+ (Final.ITERATIONS - 1) + "/part-00000";

		FileInputFormat.addInputPath(job, new Path(
				finalNormalizedRankOutputPathString));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job).delete(new Path(args[1]), true);
		JobClient.runJob(job);
	}

}
