import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Final {

	public static double EPSILON = 0.15;
	public static int ITERATIONS = 10;
	public static String SELF = "0";
	public static String POINT_TO = "1";

	// just run jobs one by one
	public static void main(String[] args) throws IOException {

		GraphCreator.main(args);
		TotalCountAndInit.main(args);
		Iteration.main(args);
		CleanupAndSorting.main(args);

		// show last 10 entries, because rank value is in decreasing order
		Path file = new Path(args[1] + "/part-00000");

		String content = "";

		ArrayList<String> invertedList = new ArrayList<String>();

		try {
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(file)));
			while ((content = br.readLine()) != null) {
				invertedList.add(content);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < 10; i++) {
			System.out.println(invertedList.get(invertedList.size() - 1 - i));
		}

	}
}
