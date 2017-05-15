package analysis;

public enum MarkType {

	Positive(1, "正面"), Negative(-1, "负面"), Neutral(0, "中性");

	private int code;
	private String display;

	MarkType(int code, String display) {
		this.code = code;
		this.display = display;
	}

	public int getCode() {
		return code;
	}

	public String getDisplay() {
		return display;
	}

}
