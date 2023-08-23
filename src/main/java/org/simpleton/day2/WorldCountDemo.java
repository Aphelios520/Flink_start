package org.simpleton.day2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorldCountDemo {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSource<String> dataSource = environment.readTextFile("src/main/resources/a.txt");
		
		FlatMapOperator<String, Tuple2<String, Integer>> mapOperator = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			
			
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				String[] split = value.split(" ");
				
				for(String s : split){
					Tuple2<String, Integer> of = Tuple2.of(s, 1);
					out.collect(of);
				}
			}
		});
		
		UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping = mapOperator.groupBy(0);
		AggregateOperator<Tuple2<String, Integer>> sum = unsortedGrouping.sum(1);
		sum.print();
		
		
	}
}
