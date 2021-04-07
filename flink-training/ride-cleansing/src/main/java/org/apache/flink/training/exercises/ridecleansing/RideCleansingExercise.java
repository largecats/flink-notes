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

package org.apache.flink.training.exercises.ridecleansing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.*;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.ExerciseBase;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;
import org.apache.flink.training.exercises.common.utils.GeoUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.*;

import java.util.logging.Filter;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import javax.xml.crypto.Data;

/**
 * The "Ride Cleansing" exercise from the Flink training in the docs.
 *
 * <p>The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 *
 */
public class RideCleansingExercise extends ExerciseBase {

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

		DataStream<TaxiRide> filteredRides = rides
				// filter out rides that do not start or stop in NYC
				.filter(new NYCFilter());

		DataStream<EnrichedRide> enrichedNYCRides = rides
//				.filter(new NYCFilter())
//				.map(new Enrichment());
				.flatMap(new NYCEnrichment());
//				.keyBy(enrichedRide -> enrichedRide.startCell);
//		enrichedNYCRides.print();

		DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides.flatMap(
				new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
					@Override
					public void flatMap(EnrichedRide value, Collector<Tuple2<Integer, Minutes>> out) throws Exception {
						if (!value.isStart) {
							Interval rideInterval = new Interval(value.startTime.toEpochMilli(), value.endTime.toEpochMilli()); // Need to convert Instant to Long
							Minutes duration = rideInterval.toDuration().toStandardMinutes();
							out.collect(new Tuple2<>(value.startCell, duration));
						}
					}
				}
		);
		minutesByStartCell
				.keyBy(value -> value.f0) // Use the first field of the tuple as key; same as value -> value.startCell
				.maxBy(1) // Use the index 1 field of the tuple as value
				.print();

		// print the filtered stream
//		printOrTest(filteredRides);

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}

	private static class NYCFilter implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {
			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon,
					taxiRide.endLat);
		}
	}

	public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
		@Override
		public EnrichedRide map(TaxiRide taxiRide) throws Exception {
			return new EnrichedRide(taxiRide);
		}
	}

	public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
		@Override
		public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
			FilterFunction<TaxiRide> valid = new NYCFilter();
			if (valid.filter(taxiRide)) {
				out.collect(new EnrichedRide(taxiRide));
			}
		}
	}

}
