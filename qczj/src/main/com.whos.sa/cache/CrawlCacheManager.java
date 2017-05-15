package cache;

import java.util.List;

public interface CrawlCacheManager {

	String[] getKeys(String cacheName);

	<T> List<T> getAll(String cacheName, Class<T> classType);

	<T> T get(String cacheName, String key, Class<T> classType);

	<T> List<T> findByAttribute(String cacheName, String attributeName,
								Object value, Class<T> classType);

	void put(String cacheName, String key, Object value);

	<T> void putAll(String cacheName, List<T> datas);

	void remove(String cacheName, String key);

	void update(String cacheName, String key, Object value);

	void removeAll(String cacheName);

	void removeAll(String cacheName, String[] keys);

	int getSize(String cacheName);

	void create(String cacheName, String[] searchNames);

	void close();

	<T> List<T> findByAttribute(String cacheName, String[] attributeName,
								Object[] value, Class<T> classType);

	<T> List<T> query(String cacheName, String[] attributeName, Object[] value,
					  String sortField, boolean isAsc, int max, Class<T> classType);

	void deleteCache(String cacheName);

}
