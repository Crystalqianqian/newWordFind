package map;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class Map_SplitWord_First extends Mapper<LongWritable, Text, Text, Text> {
	private static final Pattern DATAPATTERN = Pattern.compile("[\\pP\\pM\\pZ]");
	private static final Pattern CHINESEPATTERN = Pattern.compile("\\s*|\\t*|\\r*|\\n*");

	private static IKAnalyzer IK;
	private static TokenStream TS;
	
	private static String word = "";
	private static StringReader stringReader = null;
    private int amount = 0;
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println(">>>>>>>>>>开始初始化分词器<<<<<<<<<<");
		IK = new IKAnalyzer(false);
		try {
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(">>>>>>>>>>分词器初始化完毕<<<<<<<<<<");
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String text = value.toString();
		List<String> tokens = new ArrayList<String>();
		try {
			stringReader = new StringReader(text);
			TS = IK.tokenStream("", stringReader);
			CharTermAttribute term = TS.addAttribute(CharTermAttribute.class);
			TS.reset();
			while (TS.incrementToken()) {
				word = term.toString();
				tokens.add(word);
			}
			TS.end();
			TS = null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for (int i = 0; i < tokens.size(); i++) {
			String left = "EMPTY";
			String right = "EMPTY";
			
			if (!DATAPATTERN.matcher(tokens.get(i)).find()) {
				if (!isMessyCode(tokens.get(i))) {
					context.write(new Text("TERM" + "\t" + tokens.get(i)), new Text(tokens.get(i)));
				}
				if (i > 0) {
					if (i > 1 && !DATAPATTERN.matcher(tokens.get(i - 2)).find())
						left = tokens.get(i - 2);
					if ((i + 1) < tokens.size() && !DATAPATTERN.matcher(tokens.get(i + 1)).find())
						right = tokens.get(i + 1);
					if (!DATAPATTERN.matcher(tokens.get(i - 1)).find() 
							&& !DATAPATTERN.matcher(tokens.get(i)).find()
							&& !isMessyCode(tokens.get(i - 1)) 
							&& !isMessyCode(tokens.get(i)) 
							&& !isMessyCode(left)
							&& !isMessyCode(right)) {
						context.write(new Text("WORD" + "\t" + tokens.get(i - 1) +"/"+ tokens.get(i)),
								new Text(left + "\t" + tokens.get(i - 1) + "\t" + tokens.get(i) + "\t" + right));
						
						context.write(new Text("DWORD"+ "\t" + tokens.get(i - 1) + tokens.get(i)), new Text(tokens.get(i - 1) + tokens.get(i)));
					}
				}
				if (i > 1 
						&& !DATAPATTERN.matcher(tokens.get(i - 2)).find() 
						&& !DATAPATTERN.matcher(tokens.get(i - 1)).find() 
						&& !DATAPATTERN.matcher(tokens.get(i)).find()
						&& !isMessyCode(tokens.get(i - 2)) 
						&& !isMessyCode(tokens.get(i - 1)) 
						&& !isMessyCode(tokens.get(i)) 
						) {
					
						
						context.write(new Text("TWORD"+ "\t" +tokens.get(i-2) + tokens.get(i - 1) + tokens.get(i)), new Text(tokens.get(i-2) + tokens.get(i - 1) + tokens.get(i)));
					}
				}
			}
		amount = amount + tokens.size();
		
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		context.write(new Text("SIZE"), new Text( String.valueOf(amount)));
	}

	/**
	 * 判断字符是否是中文
	 *
	 * @param c
	 *            字符
	 * @return 是否是中文
	 */
	private boolean isChinese(char c) {
		Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
		if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
				|| ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
				|| ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
				|| ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
				|| ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
			return true;
		}
		return false;
	}

	/**
	 * 判断字符串是否是乱码
	 *
	 * @param string字符串
	 * @return 是否是乱码
	 */
	private boolean isMessyCode(String string) {
		Matcher m = CHINESEPATTERN.matcher(string);
		String after = m.replaceAll("");
		String temp = after.replaceAll("\\p{P}", "");
		char[] ch = temp.trim().toCharArray();
		float chLength = ch.length;
		float count = 0;
		for (int i = 0; i < ch.length; i++) {
			char c = ch[i];
			if (!Character.isLetterOrDigit(c)) {
				if (!isChinese(c)) {
					count = count + 1;
				}
			}
		}
		float result = count / chLength;
		if (result > 0.4) {
			return true;
		} else {
			return false;
		}
	}
}
