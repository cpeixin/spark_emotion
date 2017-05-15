package seg;

import com.chenlb.mmseg4j.Word;

import java.io.Reader;
import java.util.List;

public interface Segmentation {

	public List<Word> execute(Reader reader);

	public List<Word> execute(String s);
}
