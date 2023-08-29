package org.simpleton.day3;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @author lijiacheng@sensorsdata.cn
 * @version 1.0
 * @data 2023/8/29 11:18 AM
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		SingleOutputStreamOperator<Transaction> transaction = executionEnvironment
				.addSource(new TransactionSource())
				.name("transaction");

		SingleOutputStreamOperator<Alert> alert = transaction
				.keyBy(Transaction::getAccountId)
				.process(new KeyedProcessFunction<Long, Transaction, Alert>() {

					@Override
						public void processElement(Transaction transaction,
							KeyedProcessFunction<Long, Transaction, Alert>.Context context, Collector<Alert> collector)
							throws Exception {

						Alert alert = new Alert();
						alert.setId(transaction.getAccountId());
						collector.collect(alert);

					}
				})
				.name("fraud-detector");

		alert.addSink(new AlertSink()).name("send-alert");

		executionEnvironment.execute();


	}
}
