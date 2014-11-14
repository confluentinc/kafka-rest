package io.confluent.kafkarest.validation;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

public class ConstraintViolations {
    private ConstraintViolations() { /* singleton */ }

    public static <T> String format(ConstraintViolation<T> v) {
        return String.format("%s %s (was %s)",
                v.getPropertyPath(),
                v.getMessage(),
                v.getInvalidValue());
    }

    public static List<String> formatUntyped(Set<ConstraintViolation<?>> violations) {
        final List<String> errors = new Vector<>();
        for (ConstraintViolation<?> v : violations) {
            errors.add(format(v));
        }
        return errors;
    }

    public static ConstraintViolationException simpleException(String msg) {
        return new ConstraintViolationException(msg, new HashSet<ConstraintViolation<?>>());
    }
}