import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class Test {

	public static boolean isDouble(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
		return pattern.matcher(str).matches();
	}

	public static void main(String[] args) {
		File f = new File("D:\\test.txt");
		BufferedReader bf;

		String document_name = "";
		String document_link = "";
		int lineNumber = 0;

		try {
			bf = new BufferedReader(new FileReader(f));
			String content;
			try {
				while ((content = bf.readLine()) != null) {
					lineNumber++;
					System.out
							.println(lineNumber + "-------------------------");

					// title?
					int startIndex = content.indexOf("<title>");
					int endIndex = content.indexOf("</title>");
					String pageTitle = content.substring(startIndex + 7,
							endIndex);// 7
										// ==
										// <title>.length

					// text?
					startIndex = content.indexOf("<text>");
					endIndex = content.indexOf("</text>");
					String pageText = content.substring(startIndex + 6,
							endIndex);// 6
										// ==
										// <text>.length

					int currentLinkPageStartIndex = 0;
					String pointTo = "";

					if (!pageTitle.contains(":")) {

//						System.out.println(pageTitle + "++" + Final.SELF + "\t"
//								+ pageTitle);

						// current page pointTo
						while (currentLinkPageStartIndex < content.length()) {

							currentLinkPageStartIndex = pageText.indexOf("[[",
									currentLinkPageStartIndex);

							if (currentLinkPageStartIndex < 0)
								break;

							int currentLinkPageEndIndex = pageText.indexOf(
									"]]", currentLinkPageStartIndex);

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
							System.out.println(pointTo + "++" + Final.POINT_TO
									+ "\t" + pageTitle);

							// move to next
							currentLinkPageStartIndex = currentLinkPageEndIndex + 1;
						}
					}

				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// String s1 = "";
		// String s2 = "hahaha";
		// System.out.println(isDouble(s1) + "\t" + isDouble(s2));

	}

}
