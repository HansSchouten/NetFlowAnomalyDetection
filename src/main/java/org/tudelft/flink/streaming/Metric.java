package org.tudelft.flink.streaming;

import java.io.Serializable;

import com.fasterxml.jackson.databind.JsonNode;

public class Metric implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8078274017619893058L;
	public String word;
	public long count = 1;
	public String json;
	
	public Metric(){}
	
	public Metric(String word, long count, String json) {
		super();
		this.word = word;
		this.count = count;
		this.json = json;
	}
	
	public Metric(String word, long count) {
		this.word = word;
		this.count = count;
	}

	public static Metric fromString(String line) {
		
		Metric metric = new Metric();
		JsonNode jsonNode = null;
		if(line != null){
			jsonNode = JSONUtils.convertToJSON(line.getBytes());
		}
		if(jsonNode != null){
			metric.json = jsonNode.toString();
			metric.word = "DEFAULT.";//jsonNode.get("commonData").get("product").asText();
		}else{
			metric.word = "DEFAULT";
		}
		return metric;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj){
			return true;
		}
		if (obj == null){
			return false;
		}
		if (getClass() != obj.getClass()){
			return false;
		}
		Metric other = (Metric) obj;
		if (word == null) {
			if (other.word != null){
				return false;
			}
		} else if (!word.equals(other.word)){
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return word + " : " + count;
	}

}
