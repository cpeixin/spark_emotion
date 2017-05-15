package analysis;
import com.chenlb.mmseg4j.Word;
import cache.CacheFactory;
import common.Config;
import common.entity.SenWord;
import seg.SegFactory;
import util.DevUtil;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;

public class Analysis {

	public static final String Sentiment_Dic_CacheName = "SenMDic";

	public Analysis() {
		Config.getInstance();
	}

	public double getSimpleSentiemntValue(String word) {
		SenWord sw = CacheFactory.getCacheManager().get(
				Sentiment_Dic_CacheName, word, SenWord.class);
		if (null == sw) {
			return 0.0;
		}
		return sw.getValue();
	}

	public MarkType parse(Reader reader) {
		double sum = 0.0;
		List<Word> words = SegFactory.getSegmentation().execute(reader);
		if (!DevUtil.isNull(words)) {
			for (Word word : words) {
				if (word.getType().equals(Word.TYPE_LETTER)
						|| word.getType().equals(Word.TYPE_WORD)) {
					Double sv = getSimpleSentiemntValue(word.getString());
					sum += sv;
				}
			}
		}
		return getType(sum);
	}

	public MarkType parse(String content) {
		return parse(new StringReader(content));
	}

	public MarkType getType(double value) {
		double[] scopes = Config.getInstance().getScopes();
		if (value < scopes[0]) {
			return MarkType.Negative;
		} else if (value < scopes[1]) {
			return MarkType.Neutral;
		} else {
			return MarkType.Positive;
		}
	}
}
