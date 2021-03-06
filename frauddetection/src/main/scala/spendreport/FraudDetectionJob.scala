/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
  * Skeleton code for the DataStream code walkthrough
  */
object FraudDetectionJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    // Create execution envrionment for setting job properties, adding sources, and triggering job later
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource) // Generates an infinite stream of card transactions
//      .assignTimestampsAndWatermarks(
//        WatermarkStrategy.forBoundedOutOfOrderness[Transaction](Duration.ofMillis(0))
//          .withTimestampAssigner( // .withTimestampAssigner((txn: Transaction) => txn.getTimestamp) cannot
//            new SerializableTimestampAssigner[Transaction] {
//              override def extractTimestamp(t: Transaction, l: Long): Long = {
//                t.getTimestamp
//              }
//            }
//          )
//      )
      .name("transactions") // For debugging purpose

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId) // Partition a stream by account id
      .process(new FraudDetector) // Applies the FraudDetector operator to each element in the stream
      .name("fraud-detector")

    alerts
      .addSink(new AlertSink) // Writes to log with level INFO (for illustration purposes)
      .name("send-alerts")

    env.execute("Fraud Detection") // Executes the lazily built job
  }
}
