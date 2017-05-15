package util.file;

import util.DevUtil;
import util.log.LogUtil;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileUtil {

	public static Map<String, Double> readDic(InputStream in) {
		Map<String, Double> map = null;
		try {
			map = new HashMap<String, Double>();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;
			String[] ary;
			while ((line = br.readLine()) != null) {
				if (!DevUtil.isNull(line)) {
					ary = line.split(" ");
					map.put(ary[0], Double.parseDouble(ary[1]));
				}
			}
			close(br);
		} catch (Exception e) {
			LogUtil.getInstance().logError(e);
		}
		return map;
	}

	public static void close(Reader rd) {
		try {
			if (null != rd) {
				rd.close();
			}
		} catch (IOException e) {
			LogUtil.getInstance().logError(e);
		}
	}
}
