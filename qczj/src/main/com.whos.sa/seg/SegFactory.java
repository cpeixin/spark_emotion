package seg;

public class SegFactory {

	private SegFactory() {

	}

	public static Segmentation getSegmentation() {
		return new Segmmseg4jImpl();
	}
}
