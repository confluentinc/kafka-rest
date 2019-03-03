package io.confluent.kafkarest;

public class ScalaConsumersContext {

  private final MetadataObserver metadataObserver;
  private final ConsumerManager consumerManager;
  private final SimpleConsumerManager simpleConsumerManager;

  public ScalaConsumersContext() {
    //FIXME Create zkUtils
    metadataObserver = new MetadataObserver(null);
    consumerManager = new ConsumerManager(null, metadataObserver);
    simpleConsumerManager = new SimpleConsumerManager(null, metadataObserver, null);
  }

  public ScalaConsumersContext(MetadataObserver metadataObserver, ConsumerManager consumerManager,
      SimpleConsumerManager simpleConsumerManager) {
    this.metadataObserver = metadataObserver;
    this.consumerManager = consumerManager;
    this.simpleConsumerManager = simpleConsumerManager;
  }

  public void shutdown() {
    metadataObserver.shutdown();
    consumerManager.shutdown();
    simpleConsumerManager.shutdown();
  }

  public SimpleConsumerManager getSimpleConsumerManager() {
    return simpleConsumerManager;
  }

  public ConsumerManager getConsumerManager() {
    return consumerManager;
  }

}
