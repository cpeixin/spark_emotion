package util.log;

import org.apache.log4j.Logger;

/**
 * 公共方法
 * 
 * @author ZhangGang
 * 
 */
public class LogUtil {

	private static Logger log = Logger.getLogger(LogUtil.class);

	private static LogUtil instance = new LogUtil();

	private LogUtil() {

	}

	public static LogUtil getInstance() {
		return instance;
	}

	public void logInfo(String message) {
		log.info(message);
	}

	public void logInfo(String message, Throwable t) {
		log.info(message, t);
	}

	public void logWarn(String message) {
		log.warn(message);
	}

	public void logWarn(String message, Throwable t) {
		log.warn(message, t);
	}

	public void logFatal(String message) {
		log.fatal(message);
	}

	public void logFatal(String message, Throwable t) {
		log.fatal(message, t);
	}

	public void logError(String message) {
		log.error(message);
	}

	public void logError(Throwable t) {
		t.printStackTrace();
		log.error(t);
	}

	public void logError(String message, Throwable t) {
		t.printStackTrace();
		log.error(message, t);
	}

}
