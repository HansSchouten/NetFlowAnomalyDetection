package org.tudelft.flink.streaming;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public final class JSONUtils {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(JSONUtils.class);
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	public static String convertToString(final Object obj){
		String jsonString = null;
		try {
			jsonString = mapper.writeValueAsString(obj);
		} catch (JsonProcessingException ex) {
			LOGGER.error("*************************  Error occurred while converting to String ************************"+ApplicationException.getStackTrace(ex));
		}
		return jsonString;
	}
	
	public static JsonNode convertToJSON(final InputStream stream){
		JsonNode root = null;
		try {
			root = mapper.readTree(stream);
		} catch (Exception ex) {
			LOGGER.error("*************************  Error occurred while converting to String ************************"+ApplicationException.getStackTrace(ex));
		}
		return root;
	}
	
	public static JsonNode convertToJSON(final byte[] stream){
		JsonNode root = null;
		try {
			root = mapper.readTree(stream);
		} catch (Exception ex) {
			LOGGER.error("*************************  Error occurred while converting to String ************************"+ApplicationException.getStackTrace(ex));
		}
		return root;
	}
	 

}
