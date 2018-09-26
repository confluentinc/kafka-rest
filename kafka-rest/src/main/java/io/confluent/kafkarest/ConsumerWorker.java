/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.kafkarest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker thread for consumers that multiplexes multiple consumer operations onto a single thread.
 */
public class ConsumerWorker extends Thread {

  private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

  KafkaRestConfig config;

  AtomicBoolean isRunning = new AtomicBoolean(true);
  CountDownLatch shutdownLatch = new CountDownLatch(1);

  Queue<ConsumerReadTask> tasks = new LinkedList<ConsumerReadTask>();
  Queue<ConsumerReadTask> waitingTasks =
      new PriorityQueue<ConsumerReadTask>(1, new ReadTaskExpirationComparator());

  public ConsumerWorker(KafkaRestConfig config) {
    this.config = config;
  }

  public synchronized <KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> Future readTopic(
      ConsumerState state,
      String topic,
      long maxBytes,
      ConsumerWorkerReadCallback<ClientKeyT, ClientValueT> callback
  ) {
    ConsumerReadTask<KafkaKeyT, KafkaValueT, ClientKeyT, ClientValueT> task =
        new ConsumerReadTask<>(
            state,
            topic,
            maxBytes,
            callback
        );
    log.trace(
        "Scheduling consumer worker read worker={} task={} topic={} consumer={}",
        this,
        task,
        topic,
        state.getId()
    );
    if (!task.isDone()) {
      tasks.add(task);
      this.notifyAll();
    }
    return task;
  }

  @Override
  public void run() {
    while (isRunning.get()) {
      synchronized (this) {
        if (tasks.isEmpty()) {
          try {
            long now = config.getTime().milliseconds();
            long nextExpiration = nextWaitingExpiration();
            if (nextExpiration > now) {
              long timeout = (nextExpiration == Long.MAX_VALUE ? 0 : nextExpiration - now);
              assert (timeout >= 0);
              log.trace(
                  "Consumer worker waiting for next task worker={} timeout={}",
                  this,
                  timeout
              );
              config.getTime().waitOn(this, timeout);
            }
          } catch (InterruptedException e) {
            // Indication of shutdown
          }
        }

        long now = config.getTime().milliseconds();
        while (nextWaitingExpiration() <= now) {
          final ConsumerReadTask waitingTask = waitingTasks.remove();
          log.trace("Promoting waiting task to scheduled worker={} task={}", this, waitingTask);
          tasks.add(waitingTask);
        }

        final ConsumerReadTask task = tasks.poll();
        if (task != null) {
          log.trace("Executing consumer read task worker={} task={}", this, task);
          boolean backoff = task.doPartialRead();
          if (!task.isDone()) {
            if (backoff) {
              log.trace(
                  "Rescheduling consumer read task with backoff worker={} task={}",
                  this,
                  task
              );
              waitingTasks.add(task);
            } else {
              log.trace(
                  "Rescheduling consumer read task with immediately worker={} task={}",
                  this,
                  task
              );
              tasks.add(task);
            }
          }
        }
      }
    }
    shutdownLatch.countDown();
  }

  private long nextWaitingExpiration() {
    if (waitingTasks.isEmpty()) {
      return Long.MAX_VALUE;
    } else {
      return waitingTasks.peek().waitExpiration;
    }
  }

  public void shutdown() {
    try {
      isRunning.set(false);
      this.interrupt();
      shutdownLatch.await();
    } catch (InterruptedException e) {
      log.error("Interrupted while consumer worker thread.");
      throw new Error("Interrupted when shutting down consumer worker thread.");
    }
  }

  private static class ReadTaskExpirationComparator implements Comparator<ConsumerReadTask> {

    @Override
    public int compare(ConsumerReadTask t1, ConsumerReadTask t2) {
      if (t1.waitExpiration == t2.waitExpiration) {
        return 0;
      } else if (t1.waitExpiration < t2.waitExpiration) {
        return -1;
      } else {
        return 1;
      }
    }
  }
}

