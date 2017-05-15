package util.xml;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.io.InputStream;

public class XmlLoader {
	protected Document xml;

	protected XmlLoader(String fileName) throws Exception {
		this(new File(fileName));
	}

	protected XmlLoader(File file) throws Exception {
		this.xml = read(file);
		parse();
	}

	protected XmlLoader(InputStream in) throws Exception {
		this.xml = read(in);
		parse();
	}

	protected void parse() throws Exception {
	}

	protected Document read(File file) throws Exception {
		SAXReader reader = new SAXReader();
		Document document = null;
		try {
			document = reader.read(file);
		} catch (DocumentException e) {
			throw new Exception(e);
		}
		return document;
	}

	protected Document read(InputStream in) throws Exception {
		SAXReader reader = new SAXReader();
		Document document = null;
		try {
			document = reader.read(in);
		} catch (DocumentException e) {
			throw new Exception(e);
		}
		return document;
	}
}
