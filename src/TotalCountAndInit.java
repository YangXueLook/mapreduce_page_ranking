import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TotalCountAndInit {

	// input: (title, linkPageTitle1++linkPageTitle2++...) pair
	// output: "totalKey",1
	public static class TotalCountMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text totalKey = new Text("totalKey");

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			output.collect(totalKey, new IntWritable(1));
		}

	}

	// input: "totalKey",1...1
	// output: "totalKey",count only 1 pair
	public static class TotalCountReduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while (values.hasNext()) {
				count = count + values.next().get();
			}
			output.collect(key, new IntWritable(count));
		}
	}

	// just insert init rank value into previous result.
	// output: (title, initRank==>linkPageTitle1++linkPageTitle1++...) pair
	public static class InitMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text rankAndLinkTitlesList = new Text();
		private Text title = new Text();

		int totalCount = 0;

		public void configure(JobConf conf) {
			totalCount = Integer.parseInt(conf.get("TOTAL_COUNT"));
		}

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer st = new StringTokenizer(value.toString(), "\t");
			String pageTitle = "";

			double initRank = (double) 1 / (double) (totalCount);

			// build initRank + "==>" + links
			while (st.hasMoreTokens()) {
				pageTitle = st.nextToken().toString();
				if (st.hasMoreElements()) {
					String links = st.nextToken().toString();
					title.set(pageTitle);
					// just copy link list from previous result
					rankAndLinkTitlesList.set(initRank + "==>" + links);
					output.collect(title, rankAndLinkTitlesList);
				} else// no links
				{
					title.set(pageTitle);
					output.collect(title, new Text(String.valueOf(initRank)
							+ "==>"));
				}
			}
		}
	}

	// trivial, still output: (title,
	// initRank==>linkPageTitle1++linkPageTitle1++...)
	// it is the input for next step
	// e.g. (A, 1/3==>B++C++); (B, 1/3==>C); (C, 1/3==>)
	public static class InitReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text linkTitlesList = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				linkTitlesList.set(values.next());
				output.collect(key, linkTitlesList);
			}
		}
	}

	public static void main(String[] args) throws IOException {

		JobConf totalCount = new JobConf(TotalCountAndInit.class);
		totalCount.setJobName("TotalCount");
		totalCount.setOutputKeyClass(Text.class);
		totalCount.setOutputValueClass(IntWritable.class);
		totalCount.setMapperClass(TotalCountMap.class);
		totalCount.setReducerClass(TotalCountReduce.class);
		totalCount.setInputFormat(TextInputFormat.class);
		totalCount.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(totalCount, new Path(
				"user/hduser/output/PA2output/GraphCreator/job2/part-00000"));
		FileOutputFormat.setOutputPath(totalCount, new Path(
				"user/hduser/output/PA2output/TotalCountAndInit/TotalCount"));
		FileSystem
				.get(totalCount)
				.delete(new Path(
						"user/hduser/output/PA2output/TotalCountAndInit/TotalCount"),
						true);
		JobClient.runJob(totalCount);

		// get totalCountString from totalCount job output
		Path file = new Path(
				"user/hduser/output/PA2output/TotalCountAndInit/TotalCount/part-00000");
		String content = "";
		String totalCountString = "";
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(file)));

			while ((content = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(content);
				st.nextToken().toString();
				totalCountString = st.nextToken().toString().trim();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		JobConf init = new JobConf(TotalCountAndInit.class);
		init.setJobName("Init");
		init.setOutputKeyClass(Text.class);
		init.setOutputValueClass(Text.class);
		init.setMapperClass(InitMap.class);
		init.setReducerClass(InitReduce.class);
		init.setInputFormat(TextInputFormat.class);
		init.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(init, new Path(
				"user/hduser/output/PA2output/GraphCreator/job2/part-00000"));
		FileOutputFormat.setOutputPath(init, new Path(
				"user/hduser/output/PA2output/TotalCountAndInit/InputForRank"));
		FileSystem
				.get(init)
				.delete(new Path(
						"user/hduser/output/PA2output/TotalCountAndInit/InputForRank"),
						true);
		// pass TOTAL_COUNT to init job
		init.set("TOTAL_COUNT", totalCountString);
		JobClient.runJob(init);
	}
}
