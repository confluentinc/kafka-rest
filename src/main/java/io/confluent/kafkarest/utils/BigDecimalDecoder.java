package io.confluent.kafkarest.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BigDecimalDecoder {

    public static BigDecimal fromBytes(ByteBuffer value, int scale) {
        // always copy the bytes out because BigInteger has no offset/length ctor
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }

}
