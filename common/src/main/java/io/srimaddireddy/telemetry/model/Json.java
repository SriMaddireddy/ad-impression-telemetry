package io.srimaddireddy.telemetry.model;
import com.fasterxml.jackson.databind.ObjectMapper;
public final class Json {
 public static final ObjectMapper MAPPER=new ObjectMapper();private Json(){}
 public static String write(Object value){try{return MAPPER.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException("Cannot encode JSON",e);}}
 public static <T> T read(String value,Class<T> type){try{return MAPPER.readValue(value,type);}catch(Exception e){throw new IllegalArgumentException("Invalid JSON",e);}}
}
