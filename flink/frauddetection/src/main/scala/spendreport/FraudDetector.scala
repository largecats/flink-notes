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

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

/**
  * Skeleton code for implementing a fraud detector.
  */
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

// Dummy example: Throws alert for each transaction
//@SerialVersionUID(1L)
//class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {
//
//  @throws[Exception]
//  def processElement( // Called for every transaction event
//      transaction: Transaction,
//      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
//      collector: Collector[Alert]): Unit = {
//
//    val alert = new Alert // Creates alert on every transaction
//    alert.setId(transaction.getAccountId)
//
//    collector.collect(alert)
//  }
//}

// Actual fraud detector example: Throws alert if one large (> 500) transaction immediately follows a small one (< 1)
@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  // Initialize fault-tolerant member variables
  @transient private var flagState: ValueState[java.lang.Boolean] = _ // Stores whether a small transaction was encountered
  @transient private var timerState: ValueState[java.lang.Long] = _ // Stores state for the timer (1min timeout associated with small transactions, a small transaction followed by a large one that is > 1min later are not considered as fraud)

  // open(): Hook that registers the state before the function starts processing data. Inherited from AbstractRichFunction, parent class of KeyedProcessFunction
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor) // Create ValueState using ValueStateDescriptor

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  override def processElement(
                               transaction: Transaction,
                               context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                               collector: Collector[Alert]): Unit = {

    // Get the current state for the current key
    val lastTransactionWasSmall = flagState.value

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        // Output an alert downstream
        val alert = new Alert
        alert.setId(transaction.getAccountId)

        collector.collect(alert)
      }
      // Clean up our state
      cleanUp(context) // No matter current transaction is large or not, the pattern is finished (or broken) and we have to start afresh
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)
      val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE

      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer) // Stores timestamp in timerState
    }
  }

  // Specifies what to do when the timer fires
  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                        out: Collector[Alert]): Unit = { // onTimer() is called when a timer fires
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value // Is this why timerState is needed?
    ctx.timerService.deleteProcessingTimeTimer(timer) // Deletes the timer with the given trigger time (so timer is identified by trigger time?)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }
}
