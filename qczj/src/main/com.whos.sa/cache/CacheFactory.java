package cache;

import cache.impl.CrawlCacheManagerImpl;

public class CacheFactory {

	private static CrawlCacheManager cm = (CrawlCacheManager) new CrawlCacheManagerImpl();

	private CacheFactory() {

	}

	public static CrawlCacheManager getCacheManager() {
		return cm;
	}
}
