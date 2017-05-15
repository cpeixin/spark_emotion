package cache.impl;

import cache.CrawlCacheManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.SearchAttribute;
import net.sf.ehcache.config.Searchable;
import net.sf.ehcache.search.*;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import util.DevUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class CrawlCacheManagerImpl implements CrawlCacheManager {

	private CacheManager cacheManager;

	public CrawlCacheManagerImpl() {
		cacheManager = CacheManager.newInstance();
	}

	@Override
	public String[] getKeys(String cacheName) {
		@SuppressWarnings("unchecked")
		List<String> keys = getCache(cacheName).getKeys();
		if (null != keys && keys.size() > 0) {
			return keys.toArray(new String[keys.size()]);
		}
		return new String[0];
	}

	@Override
	public <T> List<T> getAll(String cacheName, Class<T> classType) {
		List<T> list = new LinkedList<T>();
		String[] keys = getKeys(cacheName);
		if (null != keys && keys.length > 0) {
			for (String key : keys) {
				list.add((T) get(cacheName, key, classType));
			}
		}
		return list;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(String cacheName, String key, Class<T> classType) {
		Element element = getCache(cacheName).get(key);
		if (null != element) {
			return (T) element.getObjectValue();
		}
		return null;
	}

	@Override
	public void put(String cacheName, final String key, final Object value) {
		Element element = new Element(key, (Serializable) value);
		getCache(cacheName).put(element);
	}

	@Override
	public <T> void putAll(String cacheName, List<T> datas) {
		if (!DevUtil.isNull(datas)) {
			List<Element> list = new LinkedList<Element>();
			for (T data : datas) {
				Element element = new Element(DevUtil.getUUID()
						+ System.currentTimeMillis(), (Serializable) data);
				list.add(element);
			}
			getCache(cacheName).putAll(list);
		}
	}

	@Override
	public void update(String cacheName, final String key, final Object value) {
		Element element = new Element(key, (Serializable) value);
		getCache(cacheName).replace(element);
	}

	@Override
	public void remove(String cacheName, final String key) {
		getCache(cacheName).remove(key);
	}

	@Override
	public void removeAll(String cacheName, final String[] keys) {
		if (!DevUtil.isNull(keys)) {
			List<String> keysCollection = new LinkedList<String>();
			Collections.addAll(keysCollection, keys);
			getCache(cacheName).removeAll(keysCollection);
		}
	}

	@Override
	public void removeAll(String cacheName) {
		getCache(cacheName).removeAll();
	}

	@Override
	public int getSize(String cacheName) {
		return getCache(cacheName).getKeys().size();
	}

	@Override
	public void close() {
		cacheManager.shutdown();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> findByAttribute(String cacheName,
			final String attributeName, final Object value, Class<T> classType) {
		List<T> rs = new LinkedList<T>();
		Cache cache = getCache(cacheName);
		Attribute<Object> attribute = cache.getSearchAttribute(attributeName);
		Query query = cache.createQuery();
		query.includeKeys();
		query.includeValues();
		query.addCriteria(attribute.eq(value));
		Results results = query.execute();
		if (null != results && results.size() > 0) {
			List<Result> resultList = results.all();
			for (Result aResultList : resultList) {
				rs.add((T) aResultList.getValue());
			}
		}
		if (null != results) {
			results.discard();
		}
		return rs;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> findByAttribute(String cacheName,
			final String[] attributeName, final Object[] value,
			Class<T> classType) {
		List<T> rs = new LinkedList<T>();
		Cache cache = getCache(cacheName);
		Query query = cache.createQuery();
		query.includeKeys();
		query.includeValues();
		for (int i = 0; i < attributeName.length; i++) {
			if (null == value[i]) {
				continue;
			}
			Attribute<Object> attribute = cache
					.getSearchAttribute(attributeName[i]);
			query.addCriteria(attribute.eq(value[i]));
		}
		Results results = query.execute();
		if (null != results && results.size() > 0) {
			List<Result> resultList = results.all();
			for (Result aResultList : resultList) {
				rs.add((T) aResultList.getValue());
			}
		}
		if (null != results) {
			results.discard();
		}
		return rs;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> List<T> query(String cacheName, final String[] attributeName,
			final Object[] value, String sortField, boolean isAsc, int max,
			Class<T> classType) {
		List<T> rs = new LinkedList<T>();
		Cache cache = getCache(cacheName);
		Query query = cache.createQuery();
		query.includeKeys();
		query.includeValues();
		if (!DevUtil.isNull(attributeName)) {
			for (int i = 0; i < attributeName.length; i++) {
				if (null == value[i]) {
					continue;
				}
				Attribute<Object> attribute = cache
						.getSearchAttribute(attributeName[i]);
				query.addCriteria(attribute.eq(value[i]));
			}
		}
		if (!DevUtil.isNull(sortField)) {
			Attribute<Object> attribute = cache.getSearchAttribute(sortField);
			query.addOrderBy(attribute, isAsc ? Direction.ASCENDING
					: Direction.DESCENDING);
		}
		if (max > 0) {
			query.maxResults(1);
		}
		Results results = query.execute();
		if (null != results && results.size() > 0) {
			List<Result> resultList = results.all();
			for (Result aResultList : resultList) {
				rs.add((T) aResultList.getValue());
			}
		}
		if (null != results) {
			results.discard();
		}
		return rs;
	}

	private Cache getCache(String cacheName) {
		return cacheManager.getCache(cacheName);
	}

	@Override
	public void create(String cacheName, String[] searchNames) {
		CacheConfiguration cacheConfig = new CacheConfiguration();
		cacheConfig.name(cacheName).maxEntriesLocalHeap(2000000)
				.maxEntriesLocalDisk(2000000).memoryStoreEvictionPolicy(
						MemoryStoreEvictionPolicy.LFU)
				.diskSpoolBufferSizeMB(30);
		if (!DevUtil.isNull(searchNames)) {
			Searchable searchable = new Searchable();
			for (String attributeName : searchNames) {
				SearchAttribute searchAttribute = new SearchAttribute();
				searchAttribute.setName(attributeName);
				searchable.addSearchAttribute(searchAttribute);
			}
			cacheConfig.addSearchable(searchable);
		}
		Cache cache = new Cache(cacheConfig);
		cacheManager.addCache(cache);
	}

	@Override
	public void deleteCache(String cacheName) {
		cacheManager.removeCache(cacheName);
	}

}
