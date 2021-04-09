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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	/**
	 * Main method.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public static void main(String[] args) throws Exception {

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()));

//		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
//				.keyBy(x -> x.driverId)
//				.window(TumblingEventTimeWindows.of(Time.hours(1)))
//				.process(new GetHourlyTips());
		// Alternative solution using Process Functions
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
				.keyBy(x -> x.driverId)
				.process(new PseudoWindow(Time.hours(1)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
				.windowAll(TumblingEventTimeWindows.of(Time.hours(1))) // non-keyed stream needs windowAll()
				.maxBy(2);

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	public static class PseudoWindow extends KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

		private final long durationMsec;
		private transient MapState<Long, Float> sumOfTips; // Maps timestamp for the end of each window to the sum of tips for that window

		public PseudoWindow(Time duration) {
			this.durationMsec = duration.toMilliseconds();
		}

		@Override
		public void open(Configuration conf) {
			MapStateDescriptor<Long, Float> sumDesc = new MapStateDescriptor<Long, Float>("sumOfTips", Long.class, Float.class);
			sumOfTips = getRuntimeContext().getMapState(sumDesc);
		}

		@Override
		public void processElement(
				TaxiFare fare,
				Context ctx,
				Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			long eventTime = fare.getEventTime();
			TimerService timerServie = ctx.timerService();

			if (eventTime <= timerServie.currentWatermark()) {
				// This event is late; its window has already been triggered.
			} else {
				// Compute end time of the window containing this event from eventTime.
				long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

				// Set timer to fire at endOfWindow.
				timerServie.registerEventTimeTimer(endOfWindow);

				// Add this fare's tip to the running total of that window.
				Float sum = sumOfTips.get(endOfWindow); // Get the sum of tips for this window from the timestamp for the end of this window.
				if (sum == null) {
					sum = 0.0F;
				}
				sum += fare.tip;
				sumOfTips.put(endOfWindow, sum); // Update the map state.
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			long driverId = context.getCurrentKey();
			// Look up the sum for the hour that just ended in the MapState variable
			Float sumOfTips = this.sumOfTips.get(timestamp);

			Tuple3<Long, Long, Float> result = Tuple3.of(timestamp, driverId, sumOfTips);
			out.collect(result);
			this.sumOfTips.remove(timestamp);
		}
	}

	public static class GetHourlyTips extends ProcessWindowFunction<
			TaxiFare,                  // input type
			Tuple3<Long, Long, Float>,  // output type
			Long,                         // key type
			TimeWindow> {                   // window type

		@Override
		public void process(
				Long key,
				Context context,
				Iterable<TaxiFare> fares,
				Collector<Tuple3<Long, Long, Float>> out) {

			float total = 0;
			for (TaxiFare fare: fares) {
				total += fare.tip;
			}
			out.collect(Tuple3.of(context.window().getEnd(), key, total));
		}
	}

}
