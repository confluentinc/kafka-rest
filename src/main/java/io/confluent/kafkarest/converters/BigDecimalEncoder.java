package io.confluent.kafkarest.converters;

import java.math.BigDecimal;
import java.nio.charset.Charset;

public class BigDecimalEncoder {

    static public byte[] toByteArray(BigDecimal bigDecimal, int scale) {
        // will throw an error if rounding is necessary (meaning scale is wrong)
        return bigDecimal.setScale(scale, BigDecimal.ROUND_UNNECESSARY).unscaledValue().toByteArray();
    }


    static public String toEncodedString(BigDecimal bigDecimal, int scale, int precision) {
        return new String(toByteArray(bigDecimal,scale), Charset.forName("ISO-8859-1"));
    }

}