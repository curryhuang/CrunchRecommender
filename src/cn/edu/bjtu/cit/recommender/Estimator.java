package cn.edu.bjtu.cit.recommender;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.google.common.collect.Maps;

@SuppressWarnings("serial")
public class Estimator implements Serializable {

	public static final String CLUSTER_SIZE = "clustersize";
	public static final String SCALE_FACTORS = "scalefactors";
	public static final String SIZE_FACTOR = "size";
	public static final String RECS_FACTOR = "recs";
	public static final String DELM = "::|,|\\s+";

	public static Estimator instance;

	private int clusterSize;
	private Map<String, ScaleFactor> scaleFactors;

	public Estimator(String path, int clusterSize) {
		this.clusterSize = clusterSize;
		scaleFactors = Maps.newHashMap();
		parseConfiguration(path);
	}

	public Estimator() {
		clusterSize = 1;
		scaleFactors = Maps.newHashMap();
	}

	public boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]+(.[0-9]+)?");
		return pattern.matcher(str).matches();
	}

	private void parseConfiguration(String path) {
		SAXBuilder sb = new SAXBuilder();
		Document doc;
		try {
			doc = sb.build(path);
			Element root = doc.getRootElement();
			Element sf = root.getChild("scalefactors");
			List<Element> list = sf.getChildren();
			for (Element e : list) {
				String name = e.getName();
				float size = Float.parseFloat(e.getChildText("size"));
				float recs = Float.parseFloat(e.getChildText("recs"));
				scaleFactors.put(name, new ScaleFactor(size, recs));
			}
		} catch (JDOMException | IOException e) {
			e.printStackTrace();
		}
	}

	public int getClusterSize() {
		return this.clusterSize;
	}

	public ScaleFactor getScaleFactor(String stage) {
		if (hasScaleFactor(stage)) {
			return this.scaleFactors.get(stage);
		} else {
			return new ScaleFactor(1, 1);
		}
	}

	public boolean hasScaleFactor(String stage) {
		return this.scaleFactors.containsKey(stage);
	}

	public void setClusterSize(int size) {
		this.clusterSize = size;
	}

	public static class ScaleFactor implements Serializable {
		public float sizeFactor;
		public float recsFactor;

		public ScaleFactor() {
			this(1, 1);
		}

		public ScaleFactor(float size) {
			this(size, 1);
		}

		public ScaleFactor(float size, float recs) {
			sizeFactor = size;
			recsFactor = recs;
		}

		@Override
		public boolean equals(Object other) {
			if (other == null || !(other instanceof ScaleFactor)) {
				return false;
			}
			ScaleFactor f = (ScaleFactor) other;
			return (f.sizeFactor == this.sizeFactor && f.recsFactor == this.recsFactor);
		}

		@Override
		public int hashCode() {
			HashCodeBuilder hcb = new HashCodeBuilder();
			return hcb.append(this.sizeFactor).append(this.recsFactor).toHashCode();
		}

		@Override
		public String toString() {
			return "[" + sizeFactor + "|" + recsFactor + "]";
		}
	}
}
