package org.simpleton.day1;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WikipediaAnaysis {
	public static void main(String[] args) throws Exception {
//		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//
//		DataStreamSource<WikipediaEditEvent> edit = environment.addSource(new WikipediaEditsSource());
//
//		KeyedStream<WikipediaEditEvent, String> userStream = edit.keyBy(new KeySelector<WikipediaEditEvent, String>() {
//			@Override
//			public String getKey(WikipediaEditEvent value) throws Exception {
//				return value.getUser();
//			}
//		});meWindow(Time.seconds(5))
//			.fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
//				@Override
//				public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) {
//					acc._1 = event.getUser();
//					acc._2 += event.getByteDiff();
//					return acc;
//				}
//
//		DataStream<Tuple2<String, Long>> result = userStream
//			.ti
//			});

		
		//final KeyedStream<WikipediaEditEvent, Integer> byteDiffStream = edit.keyBy(WikipediaEditEvent::getByteDiff);
		
		
	//	TypeInformation<Integer> keyType = byteDiffStream.getKeyType();
		
		
//		environment.execute();
}
}
