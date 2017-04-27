package io.confluent.kafkarest.converters;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class BigDecimalDecoder {

    // see: https://github.com/apache/avro/blob/33d495840c896b693b7f37b5ec786ac1acacd3b4/lang/java/avro/src/main/java/org/apache/avro/Conversions.java#L79
    public static BigDecimal fromBytes(ByteBuffer value, int scale) {
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    public static BigDecimal fromEncodedString(String encoded, int scale){
        return fromBytes(ByteBuffer.wrap(encoded.getBytes(Charset.forName("ISO-8859-1"))), scale);
    }

}
