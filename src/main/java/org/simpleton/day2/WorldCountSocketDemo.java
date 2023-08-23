package org.simpleton.day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WorldCountSocketDemo{
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStreamSource<String> localhost = environment.socketTextStream("hadoop102", 7777);
		
		localhost.flatMap((String value, Collector<Tuple2<String, Integer>> out) ->{
				String[] split = value.split(" ");
				for(String s : split) {
					out.collect(Tuple2.of(s, 1));
				}
			
		}).returns(Types.TUPLE(Types.STRING,Types.INT))
			.keyBy(s ->s.f0)
			.sum(1).print();
		
		environment.execute();
		
	}
	
	
}



