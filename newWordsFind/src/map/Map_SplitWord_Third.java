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

public class Map_SplitWord_Third extends Mapper<LongWritable, Text, Text, Text> {

	private static IKAnalyzer IK;
	private static TokenStream TS;
	
	private static final Pattern DATAPATTERN = Pattern.compile("[\\pP\\pM\\pZ]");
	private static final Pattern CHINESEPATTERN = Pattern.compile("\\s*|\\t*|\\r*|\\n*");
	
	private static String word = "";
	private static StringReader stringReader = null;
    private int WORD_NUMBER = 0;
	private Map<String, Integer> termMap = new HashMap<String, Integer>();
	private Map<String, Integer> wordMap = new HashMap<String, Integer>();
	private Map<String, Integer> retermMap = new HashMap<String, Integer>();
	private Map<String, Integer> dwordMap = new HashMap<String, Integer>();
	private Map<String, Integer> twordMap = new HashMap<String, Integer>();
    
    private static Logger logger=LoggerFactory.getLogger(Map_SplitWord_Third.class);
    
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
		
		
		//通过頻数和内部凝固程度排除
		for(String word:wordMap.keySet()){
			
			String[] termSplit = word.split("/");
			
			//x为没歌词的頻数
			BigDecimal x = new BigDecimal(wordMap.get(word)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP);
//			System.out.println(x);
			
			List<Integer> yList =  new ArrayList<>();
			
			List<Integer> yyList =  new ArrayList<>();
			
			List<Integer> tList = new ArrayList<>();
			tList.add(100);
			tList.add(200);
			
			//三个字的词分割有两种情况:第一种情况
			for(String term:termMap.keySet()){
				
				if (term.equals(termSplit[0])) {
					
					yList.add(termMap.get(term));
					
					for (String dword : dwordMap.keySet()) {
						
						if (dword.equals(termSplit[1]+termSplit[2])) {
							
							yList.add(dwordMap.get(dword));
						}
					}
				}
			}
			
			//三个字的词分割有两种情况:第二种情况
			for(String dword:dwordMap.keySet()){
				
				if (dword.equals(termSplit[0]+termSplit[1])) {
					
					yyList.add(dwordMap.get(dword));
					
					for (String term : termMap.keySet()) {
						
						if (term.equals(termSplit[2])) {
							
							yyList.add(termMap.get(term));
						}
					}
				}
			}
			
			
			BigDecimal y1 = new BigDecimal(yList.get(0)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y2 = new BigDecimal(yList.get(1)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			
			BigDecimal y3 = new BigDecimal(yyList.get(0)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			BigDecimal y4 = new BigDecimal(yyList.get(1)).divide(new BigDecimal(WORD_NUMBER), 16, BigDecimal.ROUND_HALF_UP) ;
			
			//			System.out.println(y1+" "+y2);
			yList = null;
			
			Double temp1 = y1.multiply(y2).doubleValue();
			Double temp2 = y3.multiply(y4).doubleValue();
			
//			System.out.println(temp);
			if (temp1 != 0 && temp2 != 0) {
				double doubleValue1 = x.divide(new BigDecimal(temp1), 26, BigDecimal.ROUND_HALF_UP).doubleValue();
				double doubleValue2 = x.divide(new BigDecimal(temp1), 26, BigDecimal.ROUND_HALF_UP).doubleValue();
//				System.out.println(doubleValue);
				
				//20以下 '有一' '了我' '了一'
				if ((doubleValue1> 20.0 && x.doubleValue() > 0.000075) ||
						(doubleValue2> 20.0 && x.doubleValue() > 0.000075)) {
					
//					wordAfterMap.put(word, x);
					
//					context.write(new Text(word), new Text(String.valueOf(x)+"\t"+String.valueOf(doubleValue)));
					context.write(new Text("RETERM" + "\t" + termSplit[0] + termSplit[1] + termSplit[2]), new Text(String.valueOf(twordMap.get(termSplit[0] + termSplit[1] + termSplit[2]))));
				}
			}
		}
		for (String term : termMap.keySet()) {
			context.write(new Text("TERM" + "\t" + term), new Text(String.valueOf(termMap.get(term))));
		}
		
		for (String term : twordMap.keySet()) {
			context.write(new Text("TWORD" + "\t" + term), new Text(String.valueOf(twordMap.get(term))));
		}
		
		for (String term : dwordMap.keySet()) {
			context.write(new Text("DWORD" + "\t" + term), new Text(String.valueOf(dwordMap.get(term))));
		}
		
		for (String term : retermMap.keySet()) {
			context.write(new Text("RETERM" + "\t" + term), new Text(String.valueOf(retermMap.get(term))));
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
//					context.write(new Text("TERM" + "\t" + tokens.get(i)), new Text(tokens.get(i)));
				}
				if (i > 2) {
					if (i > 3 && !DATAPATTERN.matcher(tokens.get(i - 4)).find())
						left = tokens.get(i - 4);
					if ((i + 1) < tokens.size() && !DATAPATTERN.matcher(tokens.get(i + 1)).find())
						right = tokens.get(i + 1);
					if (!DATAPATTERN.matcher(tokens.get(i - 3)).find() 
							&& !DATAPATTERN.matcher(tokens.get(i-2)).find()
							&& !isMessyCode(tokens.get(i - 3)) 
							&& !isMessyCode(tokens.get(i-2)) 
							&& !isMessyCode(left)
							&& !isMessyCode(right)) {
						
//						String wordTerm = tokens.get(i - 1) +"/"+ tokens.get(i);
//						for (String word : wordAfterMap.keySet()) {
//							
//							if (wordTerm.equals(word)) {
								
								context.write(new Text("WORD" + "\t" + tokens.get(i-3) + "/" + tokens.get(i - 2) + "/" +tokens.get(i - 1) + "/" + tokens.get(i)),
										new Text(left + "\t" +  tokens.get(i-3) + tokens.get(i - 2) +  tokens.get(i - 1) + tokens.get(i)+ "\t" + right));
//							}
//						
//						}
					}
				}
			}
		}
	}
	

	private void readCacheFile(Path cacheFilePath) throws IOException {
//		BufferedReader reader = new BufferedReader(new FileReader(cacheFilePath.toUri().getPath()));
		BufferedReader reader = new BufferedReader(new FileReader(cacheFilePath.toUri().getPath()));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] temp = line.split("\t");
			for (int j = 0; j < temp.length; j++) {
				if (temp[0].equals("TERM")) {
					termMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("WORD")) {
					wordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("SIZE")) {
					WORD_NUMBER = Integer.parseInt(temp[2]);
				}else if (temp[0].equals("DWORD")) {
					dwordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("TWORD")) {
					twordMap.put(temp[1], Integer.parseInt(temp[2]));
				}else if (temp[0].equals("RETERM")) {
					retermMap.put(temp[1], Integer.parseInt(temp[2]));
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
