package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

public class ProduceRequest {
    @NotEmpty
    private List<ProduceRecord> records;

    @JsonProperty
    public List<ProduceRecord> getRecords() {
        return records;
    }

    @JsonProperty
    public void setRecords(List<ProduceRecord> records) {
        this.records = records;
    }

    public static class ProduceRecord {
        private String key;
        @NotNull
        private String value;

        public ProduceRecord(@JsonProperty("key") String key, @JsonProperty("value") String value) {
            this.key = key;
            this.value = value;
        }

        public ProduceRecord(byte[] unencoded_key, byte[] unencoded_value) {
            if (unencoded_key != null)
                key = EntityUtils.encodeBase64Binary(unencoded_key);
            value = EntityUtils.encodeBase64Binary(unencoded_value);
        }

        public ProduceRecord(byte[] unencoded_value) {
            this(null, unencoded_value);
        }

        @JsonProperty
        public String getKey() {
            return key;
        }

        @JsonIgnore
        public byte[] getKeyDecoded() {
            if (key == null) return null;
            return EntityUtils.parseBase64Binary(key);
        }

        @JsonProperty
        public void setKey(String key) {
            this.key = key;
        }

        @JsonProperty
        public String getValue() {
            return value;
        }

        @JsonIgnore
        public byte[] getValueDecoded() {
            return EntityUtils.parseBase64Binary(value);
        }

        @JsonProperty
        public void setValue(String value) {
            this.value = value;
        }
    }
}
