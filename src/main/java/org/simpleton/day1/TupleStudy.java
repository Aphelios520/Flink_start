package org.simpleton.day1;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;

/**
 * @author lijiacheng@sensorsdata.cn
 * @version 1.0
 * @data 2023/8/28 3:41 PM
 */
public class TupleStudy {
	public static void main(String[] args) {

		Tuple1<String> tuple1 = Tuple1.of("a");
		System.out.println(tuple1);
		Tuple tuple = Tuple.newInstance(2);
		tuple.setField("b",1);
		System.out.println(tuple);

		Tuple1<String> test1 = new Tuple1<>("sb");
		Tuple1<String> test2 = Tuple1.of("sb");
		System.out.println(test1 == test2);
		System.out.println(test1.equals(test2));
		System.out.printf("ojb1:{},obj2:{}",test1,test2);

		Tuple3<String,Integer,Double> tuple3 = new Tuple3<>();
		tuple3.setField("a",0);
		tuple3.setField("1",1);
		System.out.println(tuple3);
		System.out.println(tuple3.f1);

		Tuple3<String, Integer, Double> x = Tuple3.of("x", 1, 0.0001);
		System.out.println(x);




	}
}
