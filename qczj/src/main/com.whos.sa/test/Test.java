package test;

import analysis.Analysis;

public class Test {

	public static void main(String[] args) {

		Analysis analysis = new Analysis();
		long s = System.currentTimeMillis();
		int em = analysis.parse("【果然没有让人失望 试驾比亚迪S7尊贵型】在国内提起自主涡轮增压发动机，比亚迪的1.5T绝对算得上是一把“好手”，得益于这台发动机的优秀表现，比亚迪旗下的S6、速锐以及G5都有着不错的销量。网页链接").getCode();
		String emotion;
		if (em == 1){
			emotion = "正面";
		}else if (em == 0){
			emotion = "中性";
		}else {
			emotion = "负面";
		}
		System.out.println(emotion);


	}
}
