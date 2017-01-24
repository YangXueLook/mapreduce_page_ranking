import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class Iteration {

	public static boolean isDouble(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
		if (str.equals(""))
			return false;
		else
			return pattern.matcher(str).matches();
	}

	// input: title, rank==>linkPageTitle1++linkPageTitle2++...
	// output1: title, linkPageTitle1++linkPageTitle2++...
	// output2: title, rank
	public static class UnnormalizedRankMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			int linkCount = 0;
			StringTokenizer st = new StringTokenizer(value.toString(), "\t");
			String pageTitle = st.nextToken().trim().toString();
			String[] rankAndLinkPageTitles = st.nextToken().trim().split("==>");
			String links = "";

			if (rankAndLinkPageTitles.length > 1) {
				double rank = Double.parseDouble(rankAndLinkPageTitles[0]);
				links = rankAndLinkPageTitles[1].trim().toString();
				if (!links.equals(" ")) {

					// output1: title, linkPageTitle1++linkPageTitle2++...
					output.collect(new Text(pageTitle), new Text(links));

					StringTokenizer st1 = new StringTokenizer(links, "++");
					linkCount = st1.countTokens();
					while (st1.hasMoreElements()) {
						String outLink = st1.nextToken().toString().trim();
						// output2: title, rank
						output.collect(
								new Text(outLink),
								new Text(Double.toString((double) rank
										/ (double) (linkCount))));
					}
				}
			} else {
				output.collect(new Text(pageTitle), new Text(""));
			}
		}
	}

	// input: title, linkPageTitle1++linkPageTitle2++.../rank
	// output title, rank==>linkPageTitle1++linkPageTitle2++... rank value is
	// updated
	public static class UnnormalizedRankReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text rankAndLinkTitlesList = new Text();

		double EPSILON = 0.0;
		int totalCount = 0;

		public void configure(JobConf conf) {
			EPSILON = Double.parseDouble(conf.get("EPSILON"));
			totalCount = Integer.parseInt(conf.get("TOTAL_COUNT"));
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			double rank = 0.0;
			String links = "";

			while (values.hasNext()) {
				String value = values.next().toString();
				// if (isDouble(value))// rank value
				// {
				// rank = rank + Double.parseDouble(value);
				// } else// links value
				// {
				// links = links + value + "++";
				// }

				try {
					rank = rank + Double.parseDouble(value);
				} catch (NumberFormatException e) {
					links = links + value + "++";
				}

			}
			double newRank = (((double) EPSILON / (double) totalCount) + (((double) 1 - (double) EPSILON) * (double) rank));
			rankAndLinkTitlesList.set(newRank + "==>" + links);

			output.collect(key, rankAndLinkTitlesList);
		}
	}

	// input: title, rank==>linkPageTitle1++linkPageTitle2++...
	// output: "totalRank", rank
	public static class RankSumMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		double rankSum = 0.0;
		private static OutputCollector<Text, Text> COMBINED_OUTPUT;

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String content = value.toString();
			// Extract rank
			StringTokenizer st = new StringTokenizer(content, "\t");
			st.nextToken().trim().toString();
			String[] rank_outLinks = st.nextToken().trim().split("==>");

			output.collect(new Text("totalRank"),
					new Text(String.valueOf(rank_outLinks[0])));

		}

	}

	// input: "totalRank", (rank1,rank2...)
	// output: "totalRank", 1/C only 1 entry
	public static class RankSumReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			double sum = 0.00;
			// only 1 value
			while (values.hasNext()) {
				sum = sum + Double.parseDouble(values.next().toString());
			}
			output.collect(key, new Text(Double.toString(sum)));
		}
	}

	// input: title, rank==>linkPageTitle1++linkPageTitle2++...
	// output: title, normalizedRank==>linkPageTitle1++linkPageTitle2++...
	public static class NormalizedRankMap extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private Text rankAndLinkTitlesList = new Text();

		double totalRank = 0.0;

		public void configure(JobConf conf) {
			totalRank = Double.parseDouble(conf.get("TOTAL_RANK"));
		}

		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringTokenizer st = new StringTokenizer(value.toString(), "==>");
			double rank = Double.parseDouble(st.nextToken().trim().toString());
			double normalizedRank = rank / totalRank;
			String linkList = "";
			if (st.hasMoreTokens()) {
				linkList = st.nextToken();
			}

			rankAndLinkTitlesList.set(normalizedRank + "==>" + linkList);
			output.collect(key, rankAndLinkTitlesList);

		}
	}

	// trivial
	public static class NormalizedRankReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text rankAndLinkTitlesList = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				rankAndLinkTitlesList.set(values.next());
				output.collect(key, rankAndLinkTitlesList);
			}
		}
	}

	public static void main(String[] args) throws IOException {

		// only used for 1st iteration
		String inputPathString = "user/hduser/output/PA2output/TotalCountAndInit/InputForRank";

		String rankSumString = "";

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

		for (int i = 0; i < Final.ITERATIONS; i++) {
			// calculate unnormalized rank from last iteration output
			JobConf unnormalizedRank = new JobConf(Iteration.class);
			unnormalizedRank.setJobName("UnnormalizedRank");
			unnormalizedRank.setOutputKeyClass(Text.class);
			unnormalizedRank.setOutputValueClass(Text.class);
			unnormalizedRank.setMapperClass(UnnormalizedRankMap.class);
			unnormalizedRank.setReducerClass(UnnormalizedRankReduce.class);
			unnormalizedRank.setInputFormat(TextInputFormat.class);
			unnormalizedRank.setOutputFormat(TextOutputFormat.class);
			unnormalizedRank.set("TOTAL_COUNT", totalCountString);
			unnormalizedRank.set("EPSILON", Double.toString(Final.EPSILON));
			FileInputFormat.setInputPaths(unnormalizedRank, new Path(
					inputPathString));
			String currentUnnormalizedRankPathString = "user/hduser/output/PA2output/Iteration/UnnormalizedRank"
					+ i;

			FileOutputFormat.setOutputPath(unnormalizedRank, new Path(
					currentUnnormalizedRankPathString));
			FileSystem.get(unnormalizedRank).delete(
					new Path(currentUnnormalizedRankPathString), true);
			JobClient.runJob(unnormalizedRank);

			// calculate rank sum
			JobConf rankSum = new JobConf(Iteration.class);
			rankSum.setJobName("RankSum");
			rankSum.setOutputKeyClass(Text.class);
			rankSum.setOutputValueClass(Text.class);
			rankSum.setMapperClass(RankSumMap.class);
			rankSum.setReducerClass(RankSumReduce.class);
			rankSum.setInputFormat(TextInputFormat.class);
			rankSum.setOutputFormat(TextOutputFormat.class);
			FileInputFormat.setInputPaths(rankSum, new Path(
					currentUnnormalizedRankPathString + "/part-00000"));
			String currentRankSumPathString = "user/hduser/output/PA2output/Iteration/RankSum"
					+ i;

			FileOutputFormat.setOutputPath(rankSum, new Path(
					currentRankSumPathString));
			FileSystem.get(rankSum).delete(new Path(currentRankSumPathString),
					true);
			JobClient.runJob(rankSum);

			// get rankSumString from totalCount job output
			file = new Path(currentRankSumPathString + "/part-00000");
			content = "";
			rankSumString = "";
			try {
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(file)));

				while ((content = br.readLine()) != null) {
					StringTokenizer st = new StringTokenizer(content);
					st.nextToken().toString();
					rankSumString = st.nextToken().toString().trim();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}

			// calculate normalized rank
			JobConf normalizedRank = new JobConf(Iteration.class);
			normalizedRank.setJobName("NormalizedRank");
			normalizedRank.setOutputKeyClass(Text.class);
			normalizedRank.setOutputValueClass(Text.class);
			normalizedRank.setMapperClass(NormalizedRankMap.class);
			normalizedRank.setReducerClass(IdentityReducer.class);
			normalizedRank.setInputFormat(KeyValueTextInputFormat.class);
			normalizedRank.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(normalizedRank, new Path(
					currentUnnormalizedRankPathString + "/part-00000"));

			String currentNormalizedRankPathString = "user/hduser/output/PA2output/Iteration/NormalizedRank"
					+ i;

			FileOutputFormat.setOutputPath(normalizedRank, new Path(
					currentNormalizedRankPathString));
			FileSystem.get(normalizedRank).delete(
					new Path(currentNormalizedRankPathString), true);

			normalizedRank.set("TOTAL_RANK", rankSumString);
			JobClient.runJob(normalizedRank);

			// update input path for next iteration
			inputPathString = currentNormalizedRankPathString + "/part-00000";
		}
	}
}
