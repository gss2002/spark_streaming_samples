package org.apache.spark.streaming.example;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.IBMMQReceiver;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.JSONObject;

public class ReadMQStream implements Serializable {

    private static final long serialVersionUID = 1L;
    String processClassName = null;
    String key = null;
    String value = null;
    JSONObject jsonOut = null;
    String sparkUser = "";
    String sparkCheckPoint="";
    SparkConf sparkConf;
    long sparkBatchDuration = 1000;
    final Properties props = new Properties();

    public void startStream(String jobName,
                            String qmgrHost, String qmgrPort, String qmgrName, String qmgrChannel, String mqQueue, String mqUser,
                            String mqPassword, String mqWait, String mqKeepMessages, long batchDuration, String mqRateLimit) {

        sparkBatchDuration = batchDuration;

        try {
            sparkConf = new SparkConf().setAppName(jobName);
            if (sparkUser.equalsIgnoreCase("")) {
                sparkUser = System.getProperty("user.name");
            }
            sparkCheckPoint = "/user/"+sparkUser+"/sparkstreaming/"+jobName;
            JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(sparkCheckPoint,
                    new Function0<JavaStreamingContext>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public JavaStreamingContext call() throws Exception {
                            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
                            JavaStreamingContext ssc = new JavaStreamingContext(jsc, new Duration(sparkBatchDuration));
                            ssc.checkpoint(sparkCheckPoint);

                            sparkUser = ssc.sparkContext().sparkUser();
                            JavaDStream<String> mqStream = ssc.receiverStream(new IBMMQReceiver(qmgrHost, Integer.parseInt(qmgrPort),
                                    qmgrName, qmgrChannel, mqQueue, mqUser, mqPassword, mqWait, mqKeepMessages, mqRateLimit));


                            mqStream.foreachRDD(rdd -> {
                                rdd.foreach(record -> {
                                    key = ((JSONObject) new JSONArray(record).get(0)).getString("key");
                                    value = ((JSONObject) new JSONArray(record).get(1)).getString("value");
                                    System.out.println("MqTimeStamp Key: "+key+" :: MQValue: "+value);
                                });
                            });


                            return ssc;
                        }
                    });

            ssc.start();
            ssc.awaitTermination();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}
