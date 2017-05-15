/**JavaWordCount
 * 
 * com.magicstudio.spark
 *
 * WordCounter.java
 *
 * dumbbellyang at 2016年8月2日 下午3:42:28
 *
 * Mail:yangdanbo@163.com Weixin:dumbbellyang
 *
 * Copyright 2016 MagicStudio.All Rights Reserved
 */
package sparkSeg;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.WindowConstants;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import scala.Tuple2;

public class WordCounter extends JFrame {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final String[] docs = new String[] { "唐诗三百首", "宋词三百首", "庄子南华经",
			"老子道德经", "孟子", "论语" };
	protected JComboBox<?> docBox;
	protected JTextField docField;
	protected JButton docSelectButton;
	protected JFileChooser chooser = new JFileChooser();

	private final String[] wordLengths = new String[] { "所有词", "一字词", "二字词",
			"三字词", "四字词", "五字词", "六字词", "七字词" };
	protected JComboBox<?> lengthsBox;

	protected JRadioButton rdoTop50;
	protected JRadioButton rdoTop100;
	protected JRadioButton rdoTop500;
	protected JRadioButton rdoTop1000;
	protected JRadioButton rdoAll;
	private final String[] methods = new String[] { "Pure Java", "MapReduce",
			"Spark Java"};
	protected JComboBox<?> methodBox;
	protected JButton countButton;

	protected JLabel lblMessage;
	protected JCheckBox chkClear;

	protected JTextPane resultPane;

	private int totalWords;
	private String curDoc;

	// 是否用户选择文档
	// 指定单词长度，0为所有长度
	private Map<String, Integer> getWordCount(String doc,
			boolean isSelected, int wordLength) {
		curDoc = doc;
		try {
			IKSegmenter seg;
			if (isSelected) {
				File file = new File(doc);
				seg = new IKSegmenter(new FileReader(file), false);
			} else {
				InputStream is = WordCounter.class.getResourceAsStream("text/"
						+ doc + ".txt");
				seg = new IKSegmenter(new InputStreamReader(is), false);
			}

			SortableMap<Integer> words = new SortableMap<Integer>();
			Lexeme lex = seg.next();

			totalWords = 0;
			while (lex != null) {
				String word = lex.getLexemeText();
				if (wordLength == 0 || word.length() == wordLength) {
					if (!words.containsKey(word)) {
						words.put(word, Integer.valueOf(1));
					} else {
						int count = words.get(word).intValue();
						words.put(word, Integer.valueOf(count + 1));
					}
				}
				lex = seg.next();
				totalWords++;
			}

			Map<String, Integer> wordCount = words.sortMapByValue(false);
			System.out.println(wordCount.size());
			for (String word : wordCount.keySet()) {
				System.out.println(word + ":" + wordCount.get(word));
			}

			return wordCount;

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private void addTextLine(String str, int count) {
		SimpleAttributeSet attrSet = new SimpleAttributeSet();
		// 循环中双数行
		if (count % 20 == 0) {
			StyleConstants.setForeground(attrSet, Color.darkGray);
		}
		// 循环中单数行
		else if (count % 10 == 0) {
			StyleConstants.setForeground(attrSet, Color.lightGray);
		}
		// 循环结束后单数行
		else if (count % 20 < 10) {
			StyleConstants.setForeground(attrSet, Color.lightGray);
		}
		// 循环结束后双数行
		else {
			StyleConstants.setForeground(attrSet, Color.darkGray);
		}

		StyleConstants.setBold(attrSet, true);
		StyleConstants.setFontSize(attrSet, 16);

		Document doc = resultPane.getDocument();
		str = str + "\n";
		try {
			doc.insertString(doc.getLength(), str, attrSet);

			str = "";
		} catch (BadLocationException e) {
			System.out.println("BadLocationException: " + e);
		}
	}

	private void showWordCountTitle(String docTitle) {
		SimpleAttributeSet attrSet = new SimpleAttributeSet();
		StyleConstants.setForeground(attrSet, Color.BLACK);
		StyleConstants.setBold(attrSet, true);
		StyleConstants.setFontSize(attrSet, 18);

		Document doc = resultPane.getDocument();

		String title = docTitle + "分词结果如下:\n";
		if (chkClear.isSelected()) {
			resultPane.setText("");
		}

		try {
			doc.insertString(doc.getLength(), title, attrSet);
		} catch (BadLocationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void showWordCount(Map<String, Integer> wordCount, int countLimit) {
		int count = 0;
		String str = "";

		showWordCountTitle(curDoc);

		for (String word : wordCount.keySet()) {
			if (str.equals("")) {
				str = word + ":" + wordCount.get(word);
			} else {
				str += "  " + word + ":" + wordCount.get(word);
			}
			count++;
			if (count % 10 == 0) {
				addTextLine(str, count);

				str = "";
			}

			if ((countLimit != 0) && (count >= countLimit)) {
				break;
			}
			// System.out.println(word + ":" + wordCount.get(word));
		}

		if ("".equals(str) == false) {
			addTextLine(str, count);
		}
	}

	private void showRDDWordCount(JavaPairRDD<String, Integer> wordCount,
			int countLimit) {
		int count = 0;
		String str = "";
		
		showWordCountTitle(curDoc);

		List<Tuple2<String, Integer>> words;
		
		words = wordCount.collect();
		
		for (Tuple2<String, Integer> word : words) {
			if (countLimit == 0 || count < countLimit) {
				if (str.equals("")) {
					str = word._1 + ":" + word._2;
				} else {
					str += "  " + word._1 + ":" + word._2;
				}
				count++;
				if (count % 10 == 0) {
					addTextLine(str, count);

					str = "";
				}
			}
		}

		if ("".equals(str) == false) {
			addTextLine(str, count);
		}
	}

	private void showTimeCost(long milliSeconds) {
		lblMessage.setText("总计分词：" + totalWords + ",  耗时：" + milliSeconds
				+ " 毫秒。");
	}

	private void createSelectionPanel(JPanel panel) {
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(1, 2, 1, 2);
		gbc.fill = GridBagConstraints.HORIZONTAL;

		// 第一行 来源文档
		gbc.gridy = 0;
		gbc.gridwidth = 1;

		gbc.gridx = 0;
		JLabel label = new JLabel("选择文档:", JLabel.RIGHT);
		panel.add(label, gbc);

		gbc.gridx = 1;
		docBox = new JComboBox<String>(docs);
		docBox.setToolTipText("请选择分词统计的来源文档");
		panel.add(docBox, gbc);

		gbc.gridx = 2;
		gbc.gridwidth = 5;
		docField = new JTextField(36);
		docField.setEditable(false);
		panel.add(docField, gbc);

		gbc.gridx = 7;
		gbc.gridwidth = 1;
		docSelectButton = new JButton("...");
		panel.add(docSelectButton, gbc);

		// 第二行 词频范围
		gbc.gridy = 1;
		gbc.gridx = 0;
		gbc.gridwidth = 1;
		label = new JLabel("词频范围:", JLabel.RIGHT);
		panel.add(label, gbc);

		gbc.gridx = 1;
		gbc.gridwidth = 1;

		// 分词长度
		lengthsBox = new JComboBox<String>(wordLengths);
		lengthsBox.setToolTipText("请选择要统计的词的长度");
		panel.add(lengthsBox, gbc);

		gbc.gridx = 2;
		gbc.gridwidth = 1;
		rdoTop50 = new JRadioButton("Top 50");
		rdoTop50.setSelected(true);
		panel.add(rdoTop50, gbc);

		gbc.gridx = 3;
		gbc.gridwidth = 1;
		rdoTop100 = new JRadioButton("Top 100");
		panel.add(rdoTop100, gbc);

		gbc.gridx = 4;
		gbc.gridwidth = 1;
		rdoTop500 = new JRadioButton("Top 500");
		panel.add(rdoTop500, gbc);

		gbc.gridx = 5;
		gbc.gridwidth = 1;
		rdoTop1000 = new JRadioButton("Top 1000");
		panel.add(rdoTop1000, gbc);

		gbc.gridx = 6;
		gbc.gridwidth = 1;
		rdoAll = new JRadioButton("全部分词");
		panel.add(rdoAll, gbc);

		ButtonGroup group = new ButtonGroup();
		group.add(rdoTop50);
		group.add(rdoTop100);
		group.add(rdoTop500);
		group.add(rdoTop1000);
		group.add(rdoAll);

		gbc.gridx = 7;
		gbc.gridwidth = 1;
		methodBox = new JComboBox<String>(methods);
		methodBox.setToolTipText("请选择分词统计的方式");
		panel.add(methodBox, gbc);

		gbc.gridx = 8;
		gbc.gridwidth = 1;
		countButton = new JButton("分词统计");
		panel.add(countButton, gbc);

		// 第三行 运行信息
		gbc.gridy = 2;
		gbc.gridwidth = 1;

		gbc.gridx = 0;
		label = new JLabel("运行信息:", JLabel.RIGHT);
		panel.add(label, gbc);

		gbc.gridx = 1;
		gbc.gridwidth = 6;
		lblMessage = new JLabel("Ready", JLabel.LEFT);
		panel.add(lblMessage, gbc);

		gbc.gridx = 7;
		gbc.gridwidth = 1;
		chkClear = new JCheckBox("清除结果");
		panel.add(chkClear, gbc);

		// 調整控件大小
		docBox.setPreferredSize(new Dimension((int) (docBox.getPreferredSize()
				.getWidth()), (int) (docField.getPreferredSize().getHeight())));
		methodBox.setPreferredSize(new Dimension((int) (methodBox
				.getPreferredSize().getWidth()), (int) (docField
				.getPreferredSize().getHeight())));
		lengthsBox.setPreferredSize(new Dimension((int) (lengthsBox
				.getPreferredSize().getWidth()), (int) (docField
				.getPreferredSize().getHeight())));

		Dimension size = new Dimension(
				(int) (countButton.getPreferredSize().getWidth()),
				(int) (docField.getPreferredSize().getHeight()));
		countButton.setPreferredSize(size);
		docSelectButton.setPreferredSize(size);
	}

	protected JPanel getSelectionPanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new GridBagLayout());

		createSelectionPanel(panel);

		docSelectButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				chooser.setDialogTitle("请选择文档:");
				chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
				if (chooser.showSaveDialog(null) == JFileChooser.APPROVE_OPTION) {
					docField.setText(chooser.getSelectedFile()
							.getAbsolutePath());
				}
			}
		});

		countButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				Date start = new Date();

				if (methodBox.getSelectedIndex() == 0) {
					pureJavaWordCount();
				} else if (methodBox.getSelectedIndex() == 1) {
					hadoopWordCount();
				} else if (methodBox.getSelectedIndex() == 2) {
					sparkJavaWordCount();
				}
				showTimeCost(new Date().getTime() - start.getTime());
			}
		});

		return panel;
	}

	protected void buildPanelLayout() {
		getContentPane().add(getSelectionPanel(), BorderLayout.NORTH);
		resultPane = new JTextPane();

		JScrollPane spane = new JScrollPane(resultPane);
		spane.setPreferredSize(new Dimension(800, 400));
		getContentPane().add(spane, BorderLayout.CENTER);
	}

	public WordCounter() {

		buildPanelLayout();

		this.setTitle("Spark大数据中文分词统计");
		this.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
		this.setResizable(false);
		this.setSize(800, 600);

		// center the form
		Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
		this.setLocation((screen.width - getWidth()) / 2,
				(screen.height - getHeight()) / 2);

		this.addWindowListener(new WindowAdapter() {
			// @Override
			public void windowClosing(WindowEvent we) {
				// TODO Auto-generated method stub
				if (JOptionPane.showConfirmDialog(WordCounter.this, "确定要退出吗?",
						"提示", JOptionPane.YES_NO_CANCEL_OPTION) == JOptionPane.YES_OPTION) {

					System.exit(0);
				}
			}
		});

		setVisible(true);
	}

	private void pureJavaWordCount() {
		// 未选择文档，使用下拉框预设文档
		Map<String, Integer> wordCount = null;
		if (docField.getText().isEmpty()) {
			wordCount = getWordCount(docBox.getSelectedItem().toString(),
					false, lengthsBox.getSelectedIndex());
		} else {
			wordCount = getWordCount(docField.getText(), true,
					lengthsBox.getSelectedIndex());
		}
		// 显示分词结果
		if (wordCount == null) {
			JOptionPane.showMessageDialog(null, "分词统计结果为空！\n请检查程序或重新选择文档！");
		} else {
			if (rdoTop50.isSelected()) {
				showWordCount(wordCount, 50);
			} else if (rdoTop100.isSelected()) {
				showWordCount(wordCount, 100);
			} else if (rdoTop500.isSelected()) {
				showWordCount(wordCount, 500);
			} else if (rdoTop1000.isSelected()) {
				showWordCount(wordCount, 1000);
			} else if (rdoAll.isSelected()) {
				showWordCount(wordCount, 0);
			}
		}
	}

	private void hadoopWordCount() {
		// 未选择文档，使用下拉框预设文档
		Map<String, Integer> wordCount = null;
		try {
			if (docField.getText().isEmpty()) {
				curDoc = docBox.getSelectedItem().toString();
				wordCount = HadoopWordCount.wordCount(curDoc,false,
						lengthsBox.getSelectedIndex());
			} else {
				curDoc = docField.getText();
				wordCount = HadoopWordCount.wordCount(curDoc, true,
						lengthsBox.getSelectedIndex());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 显示分词结果
		if (wordCount == null) {
			JOptionPane.showMessageDialog(null, "分词统计结果为空！\n请检查程序或重新选择文档！");
		} else {
			if (rdoTop50.isSelected()) {
				showWordCount(wordCount, 50);
			} else if (rdoTop100.isSelected()) {
				showWordCount(wordCount, 100);
			} else if (rdoTop500.isSelected()) {
				showWordCount(wordCount, 500);
			} else if (rdoTop1000.isSelected()) {
				showWordCount(wordCount, 1000);
			} else if (rdoAll.isSelected()) {
				showWordCount(wordCount, 0);
			}
		}
	}

	private void sparkJavaWordCount() {
		SparkWordCount app = null;
		if (docField.getText().isEmpty()) {
			curDoc = docBox.getSelectedItem().toString();
			app = new SparkWordCount(curDoc, false,
					lengthsBox.getSelectedIndex());
		} else {
			curDoc = docField.getText();
			app = new SparkWordCount(curDoc, true,
					lengthsBox.getSelectedIndex());
		}

		JavaPairRDD<String, Integer> wordCount = app.wordCount();
		// 显示分词结果
		if (wordCount == null) {
			JOptionPane.showMessageDialog(null, "分词统计结果为空！\n请检查程序或重新选择文档！");
		} else {
			wordCount = app.sortByValue(wordCount, false);

			totalWords = (int)wordCount.count();
			
			if (rdoTop50.isSelected()) {
				showRDDWordCount(wordCount, 50);
			} else if (rdoTop100.isSelected()) {
				showRDDWordCount(wordCount, 100);
			} else if (rdoTop500.isSelected()) {
				showRDDWordCount(wordCount, 500);
			} else if (rdoTop1000.isSelected()) {
				showRDDWordCount(wordCount, 1000);
			} else if (rdoAll.isSelected()) {
				showRDDWordCount(wordCount, 0);
			}
		}

		app.closeSpark(wordCount);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// wordCount();
		new WordCounter();
	}

}
