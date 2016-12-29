package map;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class Map_SplitWord_Second extends Mapper<LongWritable, Text, Text, Text> {

	private static final Pattern DATAPATTERN = Pattern.compile("[\\pP\\pM\\pZ]");
	private static final Pattern CHINESEPATTERN = Pattern.compile("\\s*|\\t*|\\r*|\\n*");
	
	private static IKAnalyzer IK;
	private static TokenStream TS;
	
	private static String word = "";
	private static StringReader stringReader = null;
    private int WORD_NUMBER = 0;
	private Map<String, Integer> termMap = new HashMap<String, Integer>();
	private Map<String, Integer> wordMap = new HashMap<String, Integer>();
	private Map<String, Integer> dwordMap = new HashMap<String, Integer>();
	private Map<String, Integer> twordMap = new HashMap<String, Integer>();
	private Map<String, BigDecimal>  wordAfterMap = new HashMap<String, BigDecimal>();
    
    private static Logger logger=LoggerFactory.getLogger(Map_SplitWord_Second.class);
    
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println(">>>>>>>>>>开始初始化分词器<<<<<<<<<<");
		IK = new IKAnalyzer(false);
		try {
			Thread.sleep(5000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		Path[] paths = context.getLocalCacheFiles();
        readCacheFile(paths[0]);
		System.out.println(">>>>>>>>>>分词器初始化完毕<<<<<<<<<<");
		
//		WORD_NUMBER = Integer.parseInt(context.getConfiguration().get("SIZE"));
//		System.out.println(WORD_NUMBER+ "WORD_NUMBER");
		//通过頻数和内部凝固程度排除
		for(String word:wordMap.keySet()){
			
			String[] termSplit = word.split("/");
			
			//x为没歌词的頻数
			BigDecimal x = new BigDecimal(wordMap.get(word)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP);
//			System.out.println(x);
			
			List<Integer> yList = null;
			
			yList = new ArrayList<>();
			System.out.println(word);
			for(String term:termMap.keySet()){
				if (term.equals(termSplit[0]) ) {
					System.out.println(term);
					
					yList.add(termMap.get(term));
					
					for(String termd:termMap.keySet()){
						if (termd.equals(termSplit[1])){
							System.out.println(termd);
							
							yList.add(termMap.get(termd));
						}
					}
				}
//				System.out.println(yList.get(0)+"   "+yList.get(1));
			}
			
//			System.out.println(yList.get(0));
			
			BigDecimal y1 = new BigDecimal(yList.get(0)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y2 = new BigDecimal(yList.get(1)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
//			System.out.println(y1+" "+y2);
			yList = null;
			
			Double temp = y1.multiply(y2).doubleValue();
//			System.out.println(temp);
			if (temp != 0) {
				double doubleValue = x.divide(new BigDecimal(temp), 26, BigDecimal.ROUND_HALF_UP).doubleValue();
				//doubleValue为内部凝固程度
//				System.out.println(doubleValue);
				//20以下 '有一' '了我' '了一'
				if (doubleValue> 20.0 && x.doubleValue() > 0.000075) {
					//取出来的两个字的新词
					wordAfterMap.put(word, x);
//					context.write(new Text(word), new Text(String.valueOf(x)+"\t"+String.valueOf(doubleValue)));
					System.out.println("RETERM" + "\t" + termSplit[0] + termSplit[1]+wordMap.get(word));
					context.write(new Text("RETERM" + "\t" + termSplit[0] + termSplit[1]), new Text(String.valueOf(wordMap.get(word))));
				}
			}
		}
		
		for (String term : termMap.keySet()) {
			context.write(new Text("TERM" + "\t" + term), new Text(String.valueOf(termMap.get(term))));
			System.out.println("TERM" + "\t" + term+termMap.get(term));
		}
		
		for (String term : twordMap.keySet()) {
			context.write(new Text("TWORD" + "\t" + term), new Text(String.valueOf(twordMap.get(term))));
			System.out.println("TWORD" + "\t" + term+twordMap.get(term));
		}
		
		for (String term : dwordMap.keySet()) {
			context.write(new Text("DWORD" + "\t" + term), new Text(String.valueOf(dwordMap.get(term))));
			System.out.println("DWORD" + "\t" + term+dwordMap.get(term));
		}
		
		context.write(new Text("SIZE" + "\t" + "总数" +"\t"+ WORD_NUMBER), new Text());
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
				}
				if (i > 1) {
					if (i > 2 && !DATAPATTERN.matcher(tokens.get(i - 3)).find())
						left = tokens.get(i - 3);
					if ((i + 1) < tokens.size() && !DATAPATTERN.matcher(tokens.get(i + 1)).find())
						right = tokens.get(i + 1);
					if (!DATAPATTERN.matcher(tokens.get(i - 2)).find() 
							&& !DATAPATTERN.matcher(tokens.get(i-1)).find()
							&& !isMessyCode(tokens.get(i - 2)) 
							&& !isMessyCode(tokens.get(i-1)) 
							&& !isMessyCode(left)
							&& !isMessyCode(right)) {
								context.write(new Text("WORD" + "\t" + tokens.get(i - 2) +"/" + tokens.get(i - 1) + "/"+ tokens.get(i)),
										new Text(left + "\t" + tokens.get(i - 2)  +  tokens.get(i - 1) + tokens.get(i) + "\t" + right));
						
					}
				}
			}
		}
		
		
			
	}
	

	private void readCacheFile(Path cacheFilePath) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(cacheFilePath.toUri().getPath()));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split("\t");
			for (int j = 0; j < temp.length; j++) {
				if (temp[0].equals("TERM")) {
					termMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("WORD")) {
					wordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("DWORD")) {
					dwordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("TWORD")) {
					twordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("SIZE")) {
					WORD_NUMBER = Integer.parseInt(temp[2]);
				}
			}
		}
		reader.close();
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
