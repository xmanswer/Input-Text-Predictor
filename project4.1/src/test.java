import org.apache.hadoop.io.Text;


public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String value = "<tr><td><a href=//ip-172-31-51-107.ec2.internal:60030/>ip-172-31-51-107.ec2.internal,60020,1447975752088</a></td><td>Thu Nov 19 23:29:12 UTC 2015</td><td>requestsPerSecond=0,";
		String[] wordArray = value.replaceAll("[^a-zA-Z]+", " ")
				.trim().split("\\s+");

		for(int i = 0; i < wordArray.length; i++) {
			StringBuilder words = new StringBuilder();
			for(int j = i; (j - i) < 5 && j < wordArray.length; j++) {
				words.append(wordArray[j].toLowerCase());
				System.out.println(words.toString());
				words.append(" ");
			}
		}
	}

}
