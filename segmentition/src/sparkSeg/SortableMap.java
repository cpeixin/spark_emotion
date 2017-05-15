/**JavaWordCount
 * 
 * com.magicstudio.spark
 *
 * SortableMap.java
 *
 * dumbbellyang at 2016年8月1日 下午10:40:50
 *
 * Mail:yangdanbo@163.com Weixin:dumbbellyang
 *
 * Copyright 2016 MagicStudio.All Rights Reserved
 */
package sparkSeg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SortableMap<T extends Comparable<? super T>> extends HashMap<String, T>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	//key比较器类 
	class MapKeyComparator implements Comparator<String>{  
		//缺省升序
		private boolean isAsc = true;
		
		MapKeyComparator(boolean isAsc){
			this.isAsc = isAsc;
		}
		
		public int compare(String str1, String str2) {  
			if (isAsc){
		        return str1.compareTo(str2);  
			}
			else{
				return str2.compareTo(str1); 
			}
		}  
	}  
	
	/** 
	 * 按key进行排序 
	 * @param isAsc 是否升序 
	 * @return Map<String, T>
	 */  
	public Map<String, T> sortMapByKey(boolean isAsc) {  
	    if (this == null || this.isEmpty()) {  
	        return null;  
	    }  
	    Map<String, T> sortedMap = new TreeMap<String, T>(new MapKeyComparator(isAsc));  
	    sortedMap.putAll(this);  
	    return sortedMap;  
	}  
	
	public Map<String, T> sortMapByKey(){
		return sortMapByKey(true);
	}
	
	//value比较器类  
	public class MapValueComparator implements Comparator<Entry<String, T>> {
		//缺省升序
		private boolean isAsc = true;
				
		MapValueComparator(boolean isAsc){
			this.isAsc = isAsc;
		}
		
	    public int compare(Entry<String, T> me1, Entry<String, T> me2) {
	    	if (isAsc){
	    		return me1.getValue().compareTo(me2.getValue());  
	    	}
	    	else{
	    		return me2.getValue().compareTo(me1.getValue()); 
	    	}
	    }
	}  
	
	/** 
	 * 按value进行排序 
	 * @param isAsc 是否升序 
	 * @return Map<String, T>
	 */  
	public Map<String, T> sortMapByValue(boolean isAsc) {  
		if (this == null || this.isEmpty()) {  
			return null;  
	    }  
		
		Map<String, T> sortedMap = new LinkedHashMap<String, T>();  
		List<Entry<String, T>> entryList = new ArrayList<Entry<String, T>>(this.entrySet());
	    Collections.sort(entryList, new MapValueComparator(isAsc));  
	    Iterator<Entry<String, T>> iter = entryList.iterator();
	        Entry<String, T> tmpEntry = null;
	    while (iter.hasNext()) {  
	        tmpEntry = iter.next();  
	        sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());  
	    }  
	    return sortedMap;  
	}  
	
	public Map<String, T> sortMapByValue() {
		return sortMapByValue(true);
	}

	public static void main(String[] args){
		SortableMap<Integer> test = new SortableMap<Integer>();
		test.put("美国", Integer.valueOf(3));
		test.put("英国", Integer.valueOf(1));
		test.put("法国", Integer.valueOf(2));
		test.put("俄罗斯", Integer.valueOf(4));
		test.put("中国", Integer.valueOf(5));
		
		Map<String,Integer> sortedMap = test.sortMapByValue(false);
		for(String key:sortedMap.keySet()){
			System.out.println(key + ":" + sortedMap.get(key));
		}
	}
}
