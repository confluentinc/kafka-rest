package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.validation.ConstraintViolations;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.io.IOException;
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
        private byte[] key;
        @NotNull
        private byte[] value;

        public ProduceRecord(@JsonProperty("key") String key, @JsonProperty("value") String value) throws IOException {
            try {
                if (key != null)
                    this.key = EntityUtils.parseBase64Binary(key);
            } catch (IllegalArgumentException e) {
                throw ConstraintViolations.simpleException("Record key contains invalid base64 encoding");
            }
            try {
                this.value = EntityUtils.parseBase64Binary(value);
            } catch (IllegalArgumentException e) {
                throw ConstraintViolations.simpleException("Record value contains invalid base64 encoding");
            }
        }

        public ProduceRecord(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public ProduceRecord(byte[] unencoded_value) {
            this(null, unencoded_value);
        }

        @JsonIgnore
        public byte[] getKey() {
            return key;
        }

        @JsonProperty("key")
        public String getKeyEncoded() {
            if (key == null) return null;
            return EntityUtils.encodeBase64Binary(key);
        }

        @JsonIgnore
        public void setKey(byte[] key) {
            this.key = key;
        }

        @JsonIgnore
        public byte[] getValue() {
            return value;
        }

        @JsonProperty("value")
        public String getValueDecoded() {
            return EntityUtils.encodeBase64Binary(value);
        }

        @JsonIgnore
        public void setValue(byte[] value) {
            this.value = value;
        }
    }
}
