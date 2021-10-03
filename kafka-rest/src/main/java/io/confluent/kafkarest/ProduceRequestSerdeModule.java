package io.confluent.kafkarest;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.SettableBeanProperty;
import com.fasterxml.jackson.databind.deser.ValueInstantiator;
import com.fasterxml.jackson.databind.deser.impl.PropertyValueBuffer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.PackageVersion;
import io.confluent.kafkarest.entities.v3.ProduceRequest;
import java.io.IOException;

public class ProduceRequestSerdeModule extends SimpleModule {

  private final String NAME = "ProduceRequestSerdeModule";
  private static final long serialVersionUID = 1L;

  public ProduceRequestSerdeModule() {
    super(PackageVersion.VERSION);
    this.addValueInstantiator(
        ProduceRequest.class, new ProduceRequestValueInstantiator(ProduceRequest.class));
  }

  private class ProduceRequestValueInstantiator extends ValueInstantiator.Base {

    public ProduceRequestValueInstantiator(Class<?> type) {
      super(type);
    }

    @Override
    public Object createFromObjectWith(DeserializationContext ctxt,
                                       SettableBeanProperty[] props, PropertyValueBuffer buffer)
        throws IOException
    {
      System.out.println("Got here");
      return super.createFromObjectWith(ctxt, buffer.getParameters(props));
    }

    @Override
    public Object createFromString(DeserializationContext ctxt, String value) throws IOException {
      System.out.println("String length:" + value.length());
      return super.createFromString(ctxt, value);
    }
  }
}
