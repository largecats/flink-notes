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

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * <p>The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */

public class LongRidesExercise extends ExerciseBase {

	static long ONE_HOUR = 60 * 60 * 1000L;

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
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

		DataStream<TaxiRide> longRides = rides
				.keyBy((TaxiRide ride) -> ride.rideId)
				.process(new MatchFunction());

		printOrTest(longRides);

		env.execute("Long Taxi Rides");
	}

	public static class MatchFunction extends KeyedProcessFunction<Long, TaxiRide, TaxiRide> {

		ValueState<TaxiRide> rideState;

		@Override
		public void open(Configuration config) throws Exception {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("rideState", TaxiRide.class));
		}

		@Override
		public void processElement(TaxiRide ride, Context context, Collector<TaxiRide> out) throws Exception {
			TimerService timerService = context.timerService();
			TaxiRide previousRide = rideState.value();
			if (previousRide != null) { // ride is no the first START/END event associated with the current key
				/*
				* Alternatively: We kill the timer here if we have seen both START and END events, so that this case
				* does not make it to the onTimer() call.
				* */
//				if (!ride.isStart) {
//					timerService.deleteEventTimeTimer(previousRide.startTime.toEpochMilli() + LongRidesExercise.ONE_HOUR * 2);
//				}
				rideState.clear(); // Both START and END events have been seen, so can clear the rideState
			} else { // ride is the first START/END event associated with current key
				rideState.update(ride);
				if (ride.isStart) { // if ride is a START event, set timer for 2 hours after the ride's start time
					timerService.registerEventTimeTimer(ride.startTime.toEpochMilli() + LongRidesExercise.ONE_HOUR * 2);
				}
				// If ride is an END event, no timer is set and so onTimer() won't be called on this key
			}
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<TaxiRide> out) throws Exception {

			/*
			* START and END: rideState is null, onTimer() is called
			* END and START: rideState is null, onTimer() is not called (since no timer is set)
			* START without END: rideState is not null, onTimer() is called
			* END without START: rideState is not null, onTimer() is not called (since no timer is set)
			* */
			if (rideState.value() != null) {
				out.collect(rideState.value());
				rideState.clear();
			}

			/*
			* Alternatively: If we have killed the timer when both START and END are seen in processElement(), then
			* we can directly emit any ride events that made to onTimer().
			* */
//			out.collect(rideState.value());
//			rideState.clear();
		}
	}
}
