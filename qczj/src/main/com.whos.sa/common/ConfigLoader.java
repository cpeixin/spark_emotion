package common;

import org.dom4j.Element;
import util.log.LogUtil;
import util.xml.XmlLoader;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ConfigLoader extends XmlLoader {

	public ConfigLoader(String fileName) throws Exception {
		super(fileName);
	}

	public ConfigLoader(File file) throws Exception {
		super(file);
	}

	public ConfigLoader(InputStream in) throws Exception {
		super(in);
	}

	private Map<String, Object> attr;

	public void parse() throws Exception {
		Element root = xml.getRootElement();
		try {
			attr = new HashMap<String, Object>();
			if (!root.getName().equals("Semantic-Analysis")) {
				return;
			}
			Element scopeElement = root.element("scope");
			attr.put("scope", scopeElement.getTextTrim());
		} catch (Exception e) {
			LogUtil.getInstance().logError("parse XML error", e);
		}
	}

	public Object getConfig(String key) {
		return attr.get(key);
	}
}
