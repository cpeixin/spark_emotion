package common;

import analysis.Analysis;
import cache.CacheFactory;
import common.entity.SenWord;
import util.file.FileUtil;
import util.log.LogUtil;

import java.util.Map;
import java.util.Set;

public class Config {

	private static final String Sentiment_Dic_Path = "config/dictionary/BosonNLP_sentiment_score.txt";
	private static final String Config_Path = "config/config.xml";

	private static Config instance = new Config();

	private double[] scopes;

	private Config() {
		loadConfig();
		createCache();
	}

	public static Config getInstance() {
		return instance;
	}

	private void loadConfig() {
		try {
			ConfigLoader loader = new ConfigLoader(this.getClass().getResourceAsStream(Config_Path));
			String s = (String) loader.getConfig("scope");
			String[] sa = s.split(",");
			scopes = new double[sa.length];
			for (int i = 0; i < scopes.length; i++) {
				scopes[i] = Double.parseDouble(sa[i]);
			}
		} catch (Exception e) {
			LogUtil.getInstance().logError(e);
		}
	}

	private void createCache() {
		CacheFactory.getCacheManager().create(Analysis.Sentiment_Dic_CacheName, null);
		Map<String, Double> dic = FileUtil.readDic(this.getClass().getResourceAsStream(Sentiment_Dic_Path));
		Set<String> keys = dic.keySet();
		for (String k : keys) {
			SenWord sw = new SenWord();
			sw.setWord(k);
			sw.setValue(dic.get(k));
			CacheFactory.getCacheManager().put(Analysis.Sentiment_Dic_CacheName, k, sw);
		}
	}

	public double[] getScopes() {
		return scopes;
	}

}
