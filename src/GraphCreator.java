import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class GraphCreator {

	// input: original file
	// output: (title++SELF/POINT_TO, linkPageTitle) pair
	public static class MapOne extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text title = new Text();
		private Text linkTitle = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String content = value.toString();

			// title?
			int startIndex = content.indexOf("<title>");
			int endIndex = content.indexOf("</title>");
			String pageTitle = content.substring(startIndex + 7, endIndex);// 7
																			// ==
																			// <title>.length

			// text?
			startIndex = content.indexOf("<text>");
			endIndex = content.indexOf("</text>");
			String pageText = content.substring(startIndex + 6, endIndex);// 6
																			// ==
																			// <text>.length

			int currentLinkPageStartIndex = 0;
			String pointTo = "";

			if (!pageTitle.contains(":")) {
				// current title
				title.set(pageTitle + "++" + Final.SELF);
				linkTitle.set(pageTitle);
				output.collect(title, linkTitle);

				// current page pointTo
				while (currentLinkPageStartIndex < content.length()) {

					currentLinkPageStartIndex = pageText.indexOf("[[",
							currentLinkPageStartIndex);

					if (currentLinkPageStartIndex < 0)
						break;

					int currentLinkPageEndIndex = pageText.indexOf("]]",
							currentLinkPageStartIndex);

					if (currentLinkPageEndIndex < 0)
						break;

					String text = pageText.substring(
							currentLinkPageStartIndex + 2,
							currentLinkPageEndIndex);

					if (text.indexOf(":") >= 0) {
						currentLinkPageStartIndex = currentLinkPageEndIndex + 1;
						continue;
					}

					if (text.indexOf("#") >= 0) {
						text = text.substring(0, text.indexOf("#"));
					}

					if (text.indexOf("|") >= 0) {
						text = text.substring(0, text.indexOf("|"));
					}

					if (text.length() == 0) {
						currentLinkPageStartIndex = currentLinkPageEndIndex + 1;
						continue;
					}

					pointTo = text.trim();
					title.set(pointTo + "++" + Final.POINT_TO);
					output.collect(title, linkTitle);

					// move to next
					currentLinkPageStartIndex = currentLinkPageEndIndex + 1;
				}
			}
		}
	}

	// input: title++SELF/POINT_TO, linkPageTitleList
	// output: (title++SELF/POINT_TO, linkPageTitle) pair

	public static class ReduceOne extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text title = new Text();
		private Text linkTitle = new Text();

		private String currentTitle = "";
		private String currentLinkTitle = "";

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String selfToken = "";
			StringTokenizer st = new StringTokenizer(key.toString(), "++");
			String pageTitle = st.nextToken().trim();

			while (st.hasMoreElements()) {
				selfToken = st.nextToken();
			}

			// self => 1 to 1
			if (selfToken.equals(Final.SELF)) {
				currentTitle = pageTitle;
				currentLinkTitle = values.next().toString();
				title.set(currentLinkTitle + "++");
				linkTitle.set(currentLinkTitle);

				output.collect(title, linkTitle);

			} else// 1 to n
			{

				if (pageTitle.equals(currentTitle)) {
					while (values.hasNext()) {
						title.set(values.next().toString() + "++");
						linkTitle.set(currentLinkTitle);
						output.collect(title, linkTitle);
					}
				}

			}
		}
	}

	// just remove SELF/POINT_TO
	public static class MapTwo extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text title = new Text();
		private Text linkTitle = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String content = value.toString();
			StringTokenizer st = new StringTokenizer(content, "++");
			title.set(st.nextToken().trim());
			linkTitle.set(st.nextToken().trim());
			output.collect(title, linkTitle);
		}
	}

	// input: title, linkPageTitleList
	// output: (title, linkPageTitle1++linkPageTitle1++...) pair
	public static class ReduceTwo extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text linkTitle = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String str = "";
			HashSet<String> hs = new HashSet<String>();
			String linkPageTitle = "";

			while (values.hasNext()) {
				linkPageTitle = values.next().toString();

				if (linkPageTitle.equals(key.toString()))
					continue;

				if (hs.contains(linkPageTitle))
					continue;
				hs.add(linkPageTitle);

				str = str + linkPageTitle + "++";

			}
			linkTitle.set(str);
			output.collect(key, linkTitle);
		}
	}

	public static void main(String[] args) throws IOException {

		JobConf job1 = new JobConf(GraphCreator.class);
		job1.setJobName("GraphCreator1");
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(MapOne.class);
		job1.setReducerClass(ReduceOne.class);
		job1.setInputFormat(TextInputFormat.class);
		job1.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(
				"user/hduser/output/PA2output/GraphCreator/job1/"));
		FileSystem.get(job1).delete(
				new Path("user/hduser/output/PA2output/GraphCreator/job1/"),
				true);
		JobClient.runJob(job1);

		JobConf job2 = new JobConf(GraphCreator.class);
		job2.setJobName("GraphCreator2");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(MapTwo.class);
		job2.setReducerClass(ReduceTwo.class);
		job2.setInputFormat(TextInputFormat.class);
		job2.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(
				"user/hduser/output/PA2output/GraphCreator/job1/part-00000"));
		FileOutputFormat.setOutputPath(job2, new Path(
				"user/hduser/output/PA2output/GraphCreator/job2/"));
		FileSystem.get(job2).delete(
				new Path("user/hduser/output/PA2output/GraphCreator/job2/"),
				true);
		JobClient.runJob(job2);
	}
}
