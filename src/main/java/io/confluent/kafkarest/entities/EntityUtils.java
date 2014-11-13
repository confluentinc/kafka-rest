package io.confluent.kafkarest.entities;

import javax.xml.bind.DatatypeConverter;

public class EntityUtils {

    public static byte[] parseBase64Binary(String data) throws IllegalArgumentException {
        try {
            return DatatypeConverter.parseBase64Binary(data);
        }
        catch (ArrayIndexOutOfBoundsException e) {
            // Implementation can throw index error on invalid inputs, make sure all known parsing issues get converted to
            // illegal argument error
            throw new IllegalArgumentException(e);
        }
    }

    public static String encodeBase64Binary(byte[] data) {
        return DatatypeConverter.printBase64Binary(data);
    }
}
