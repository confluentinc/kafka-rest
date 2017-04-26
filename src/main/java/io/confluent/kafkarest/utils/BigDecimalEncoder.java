package io.confluent.kafkarest.utils;

import java.math.BigDecimal;
import java.nio.charset.Charset;

public class BigDecimalEncoder {

    static public String bytesAsUtf8(BigDecimal bi, int scale, int precision) {
        // TODO: perform some scale / precision validation
        return new String(bi.unscaledValue().toByteArray(), Charset.forName("UTF-8"));
    }

}