package common.entity;

import java.io.Serializable;

public class SenWord implements Serializable {

	private static final long serialVersionUID = 2007312045353296548L;

	private String word;
	private double value;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

}
