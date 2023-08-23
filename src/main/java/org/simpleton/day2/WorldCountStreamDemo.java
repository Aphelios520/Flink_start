package org.simpleton.day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountStreamDemo {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStreamSource<String> streamSource = environment.readTextFile("src/main/resources/a.txt");
		
		SingleOutputStreamOperator<Tuple2<String, Integer>> worldAndOne= streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				String[] split = value.split(" ");
				for (String s : split) {
					Tuple2<String, Integer> of = Tuple2.of(s, 1);
					out.collect(of);
				}
			}
		});
		
		KeyedStream<Tuple2<String, Integer>, String> keyedStream = worldAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
			@Override
			public String getKey(Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		});
		
		SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);
		
		sum.print();
		
		environment.execute();
		
		
	}
}
