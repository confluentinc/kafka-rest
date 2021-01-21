///*
// * Copyright 2020 Confluent Inc.
// *
// * Licensed under the Confluent Community License (the "License"); you may not use
// * this file except in compliance with the License.  You may obtain a copy of the
// * License at
// *
// * http://www.confluent.io/confluent-community-license
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package io.confluent.kafkarest.controllers;
//
//import io.confluent.kafkarest.controllers.AbstractConsumerLagManager.MemberId;
//import io.confluent.kafkarest.entities.ConsumerGroup;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import org.apache.kafka.clients.admin.ConsumerGroupDescription;
//import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.common.TopicPartition;
//
//public interface ConsumerOffsetsDao {
//
//  CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> getCurrentOffsets(
//      String consumerGroupId);
//
//  CompletableFuture<Map<TopicPartition, ListOffsetsResultInfo>> getLatestOffsets(
//      Map<TopicPartition, OffsetAndMetadata> currentOffsets);
//
//  Map<TopicPartition, MemberId> getMemberIds(
//      ConsumerGroupDescription consumerGroupDescription);
//
//  Map<TopicPartition, MemberId> getMemberIds(
//      ConsumerGroup consumerGroup);
//
//  long getCurrentOffset(Map<TopicPartition, OffsetAndMetadata> map, TopicPartition tp);
//
//  long getOffset(Map<TopicPartition, ListOffsetsResultInfo> map, TopicPartition tp);
//}
