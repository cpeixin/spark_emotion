package util;

import util.log.LogUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * 全局工具类,静态方法
 * 
 * @author ZhangGang
 */
public class DevUtil {

	private DevUtil() {

	}

	public static final String CRYPTION_DEFAULT_KEY = "solyee";

	public static final String STRING_SEPARATOR = ",";

	public static boolean isNull(String str) {
		return null == str || str.trim().equals("");
	}

	public static boolean isNull(String... str) {
		if (null == str) {
			return true;
		} else if (str.length == 0) {
			return true;
		} else {
			for (String s : str) {
				if (isNull(s)) {
					return true;
				}
			}
			return false;
		}
	}

	public static boolean isNull(Collection<?> collection) {
		return null == collection || collection.isEmpty();
	}

	public static boolean isNull(Object[] array) {
		return null == array || array.length == 0;
	}

	public static String toString(List<String> list) {
		return toString(list, STRING_SEPARATOR);
	}

	public static String toString(String[] array) {
		return toString(array, STRING_SEPARATOR);
	}

	public static String toString(List<String> list, String separator) {
		if (null == list || list.isEmpty()) {
			return null;
		}
		return toString(list.toArray(new String[list.size()]), separator);
	}

	public static String toString(String[] array, String separator) {
		if (null == array || array.length == 0) {
			return null;
		}
		StringBuilder buff = new StringBuilder();
		for (String s : array) {
			buff.append(s).append(separator);
		}
		return buff.substring(0, buff.length() - 1);
	}

	public static boolean isInList(List<String> list, String s) {
		if (null == list || list.isEmpty())
			return false;
		for (String item : list) {
			if (item.equals(s)) {
				return true;
			}
		}
		return false;
	}

	public static void closeInputStream(InputStream in) {
		try {
			if (null != in) {
				in.close();
			}
		} catch (IOException e) {
			LogUtil.getInstance().logError("closeInputStream error", e);
		}
	}

	public static String getUUID() {
		return UUID.randomUUID().toString().replace("-", "").toUpperCase();
	}

	public static File getClassPathFile(String path) {
		return new File(DevUtil.class.getClassLoader().getResource(path)
				.getPath());
	}

	public static URL getClassPathURL(String path) {
		return DevUtil.class.getClassLoader().getResource(path);
	}

	public static String simpleCryption(String inStr) {
		return simpleCryption(inStr, CRYPTION_DEFAULT_KEY);
	}

	public static String simpleCryption(String inStr, String key) {
		char[] a = inStr.toCharArray();
		char[] k = key.toCharArray();
		for (int i = 0; i < a.length; i++) {
			for (int j = 0; j < k.length; j++) {
				a[i] = (char) (a[i] ^ k[j]);
			}
		}
		return new String(a);
	}

	public static String toUpperCaseFisrtWord(String input) {
		return input.replaceAll(input.charAt(0) + "", (input.charAt(0) + "")
				.toUpperCase());
	}

	public static String toLowerCaseFisrtWord(String input) {
		return input.replaceAll(input.charAt(0) + "", (input.charAt(0) + "")
				.toLowerCase());
	}

	public static String filterOffUtf8Mb4(String text) {
		String charSet = "UTF-8";
		try {
			byte[] bytes = text.getBytes(charSet);
			ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
			int i = 0;
			while (i < bytes.length) {
				short b = bytes[i];
				if (b > 0) {
					buffer.put(bytes[i++]);
					continue;
				}
				b += 256;
				if ((b ^ 0xC0) >> 4 == 0) {
					buffer.put(bytes, i, 2);
					i += 2;
				} else if ((b ^ 0xE0) >> 4 == 0) {
					buffer.put(bytes, i, 3);
					i += 3;
				} else if ((b ^ 0xF0) >> 4 == 0) {
					i += 4;
				}
			}
			buffer.flip();
			return new String(buffer.array(), charSet).trim();
		} catch (Exception e) {
			return "";
		}
	}

	public static void deleteFile(File d) {
		if (!d.exists())
			return;
		if (d.isDirectory()) {
			for (File file : d.listFiles()) {
				deleteFile(file);
			}
			d.delete();
		} else {
			d.delete();
		}
	}

	public static void deleteFile(String path) {
		deleteFile(new File(path));
	}

	public static void deleteFiles(List<String> paths) {
		if (!DevUtil.isNull(paths)) {
			for (String s : paths) {
				deleteFile(s);
			}
		}
	}

	public static float getCostSecond(long s, long e) {
		return (float) (e - s) / 1000;
	}
}