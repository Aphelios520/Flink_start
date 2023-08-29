package org.simpleton.day3;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.io.IOException;

/**
 * @author lijiacheng@sensorsdata.cn
 * @version 1.0
 * @data 2023/8/29 11:18 AM
 */
public class FraudDetectionJob3 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<Transaction> transaction = executionEnvironment
				.addSource(new TransactionSource())
				.name("transaction");

		SingleOutputStreamOperator<Alert> alert = transaction
				.keyBy(Transaction::getAccountId)
				.process(new KeyedProcessFunction<Long, Transaction, Alert>() {
					private transient ValueState<Boolean> flagState;
					private transient ValueState<Long> timerState;

					@Override
					public void open(Configuration parameters) throws Exception {
						ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
								"flag",
								Types.BOOLEAN);
						flagState = getRuntimeContext().getState(flagDescriptor);
						ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("time-state", Types.LONG);
						timerState = getRuntimeContext().getState(stateDescriptor);
					}

					@Override
					public void processElement(Transaction transaction,
							Context context, Collector<Alert> collector)
							throws Exception {
						
						Boolean value = flagState.value();
						if(value != null){
							if(transaction.getAmount() > 500){
								Alert alert = new Alert();
								alert.setId(transaction.getAccountId());
								collector.collect(alert);
							}
							cleanUp(context);
						}
						
						if(transaction.getAmount() < 10){
							flagState.update(true);
							long time = context.timerService().currentProcessingTime() + 60 *1000 ;
							context.timerService().registerProcessingTimeTimer(time);
							timerState.update(time);
						}
						
					}
					
					@Override
					public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
						flagState.clear();
						timerState.clear();
					}
					
					public void cleanUp(Context context) throws IOException {
						Long timer = timerState.value();
						context.timerService().deleteProcessingTimeTimer(timer);
						
						flagState.clear();
						timerState.clear();
					}
				})
				.name("fraud-detector");

		alert.addSink(new AlertSink()).name("send-alert");

		executionEnvironment.execute();


	}
}
