package seg;

import com.chenlb.mmseg4j.*;
import util.log.LogUtil;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class Segmmseg4jImpl implements Segmentation {


	public List<Word> execute(Reader reader) {
		List<Word> list = null;
		Seg seg = null;
		try {
			Dictionary dic = Dictionary.getInstance();
			seg = new ComplexSeg(dic);
			MMSeg mmSeg = new MMSeg(reader, seg);
			Word word = null;
			while ((word = mmSeg.next()) != null) {
				if (null == list) {
					list = new ArrayList<Word>();
				}
				list.add(word);
			}
		} catch (Exception e) {
			LogUtil.getInstance().logError(e);
			list = null;
		}
		return list;
	}

	@Override
	public List<Word> execute(String s) {
		return execute(new StringReader(s));
	}

}
