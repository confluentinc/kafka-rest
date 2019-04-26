/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */


package io.confluent.kafkarest.integration;

import kafka.admin.AclCommand;

import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class SecureTestUtils {

  public static void setProduceACls(String zkConnect, String topic, String user) {
    List<String> aclArgs = new ArrayList<>();

    Collections.addAll(aclArgs, ("--authorizer kafka.security.auth.SimpleAclAuthorizer "
                                 + "--authorizer-properties  zookeeper.connect=" + zkConnect
                                 + " --topic " + topic + " --add --producer "
                                 + " --allow-principal ").split("\\s+"));
    aclArgs.add("User:" + user);
    AclCommand.main(aclArgs.toArray(new String[0]));
  }

  public static void removeProduceACls(String zkConnect, String topic, String user) {
    List<String> aclArgs = new ArrayList<>();

    Collections.addAll(aclArgs, ("--authorizer kafka.security.auth.SimpleAclAuthorizer "
                                 + "--authorizer-properties  zookeeper.connect=" + zkConnect
                                 + " --topic " + topic + " --remove --producer "
                                 + " --allow-principal ").split("\\s+"));
    aclArgs.add("User:" + user);
    AclCommand.main(aclArgs.toArray(new String[0]));
  }

  public static void setConsumerACls(
      String zkConnect, String topic, String user,
      String group
  ) {
    List<String> aclArgs = new ArrayList<>();

    Collections.addAll(aclArgs, ("--authorizer kafka.security.auth.SimpleAclAuthorizer "
                                 + "--authorizer-properties  zookeeper.connect=" + zkConnect
                                 + " --topic " + topic + " --add --consumer "
                                 + " --allow-principal ").split("\\s+"));
    aclArgs.add("User:" + user);
    aclArgs.add("--group");
    aclArgs.add(group);
    AclCommand.main(aclArgs.toArray(new String[0]));
  }
}
