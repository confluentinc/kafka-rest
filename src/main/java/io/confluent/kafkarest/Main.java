/**
 * Copyright 2014 Confluent Inc.
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
 */
package io.confluent.kafkarest;

import org.eclipse.jetty.server.Server;
import java.io.IOException;

public class Main {
    /**
     * Starts an embedded Jetty server running the REST server.
     */
    public static void main(String[] args) throws IOException {
        Server server = KafkaRestServer.createServer();

        // Finally, run the server
        try {
            server.start();
            System.out.println("Server started, listening for requests...");
            server.join();
        } catch (Exception e) {
            System.err.println("Server died unexpectedly: " + e.toString());
        }
    }
}

