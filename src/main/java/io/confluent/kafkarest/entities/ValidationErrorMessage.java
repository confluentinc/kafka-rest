package io.confluent.kafkarest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafkarest.validation.ConstraintViolations;

import javax.validation.ConstraintViolation;
import java.util.List;
import java.util.Set;

public class ValidationErrorMessage {
    private final List<String> errors;

    public ValidationErrorMessage(Set<ConstraintViolation<?>> errors) {
        this.errors = ConstraintViolations.formatUntyped(errors);
    }

    @JsonProperty
    public List<String> getErrors() {
        return errors;
    }
}