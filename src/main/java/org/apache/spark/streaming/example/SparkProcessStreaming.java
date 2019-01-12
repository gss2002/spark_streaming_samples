package org.apache.spark.streaming.example;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.spark.streaming.utils.SparkCredentialProvider;
import org.apache.spark.streaming.utils.SparkStreamingParser;

import java.io.IOException;

public class SparkProcessStreaming {
    static Options options;
    static CommandLine cmd;
    static String jobName;

    static String qmgrHost;
    static String qmgrPort;
    static String qmgrName;
    static String qmgrChannel;
    static String mqQueue;
    static String mqUsername;
    static String mqPassword;
    static String mqPasswordAlias;
    static String mqHadoopCredPath;
    static String mqKeepMessages = "false";
    static String mqWaitTime = "5000";
    static String mqRateLimit = "5000";

    static String processClassName;
    static String output;
    static String input;
    static long batchDuration = 1000;

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        String[] otherArgs = null;
        try {
            otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        } catch (IOException e4) {
            // TODO Auto-generated catch block
            e4.printStackTrace();
        }
        options = new Options();
        options.addOption("jobName", true, "Spark JobName --jobName MQSparkStreaming");
        options.addOption("input", true, "Input Type --input (ibmmq, kafka)");
        options.addOption("output", true, "Output Type --output (ibmmq, kafka)");

        options.addOption("mq_qmgr_host", true, "IBM MQ QMGR Host --mq_qmgr_host zos.example.com");
        options.addOption("mq_qmgr_port", true, "IBM MQ QMGR Port --mq_qmgr_port 1414");
        options.addOption("mq_qmgr_name", true, "IBM MQ QMGR Name --mq_qmgr_name mqp1");
        options.addOption("mq_qmgr_channel", true, "IBM MQ QMGR Channel --mq_qmgr_channel SPARK.MQ.CLIENT");
        options.addOption("mq_queue", true, "IBM MQ Queue --mq_queue SPARK.MQ.QUEUE");
        options.addOption("mq_keep_messages", false, "IBM MQ Keep Messages --mq_keep_messages");
        options.addOption("mq_wait_time", true, "IBM MQ Wait Interval --mq_wait_time 5000 (default)");
        options.addOption("mq_max_batch_rate", true, "IBM MQ Max Batch Rate --mq_max_batch_rate 5000 (default)");
        options.addOption("mq_username", true, "IBM MQ Username --mq_username mquser");
        options.addOption("mq_password", true, "IBM MQ Password --mq_password password.alias");
     //   options.addOption("mq_hadoop_cred_path", true,
      //          "IBM MQ Hadoop Cred Path --mq_hadoop_cred_path /user/username/credstore.jceks");
        options.addOption("batchDuration", true, "Spark batchDuration --batchDuration 1000 (default) milliseconds");
        options.addOption("help", false, "Display help");

        CommandLineParser parser = new SparkStreamingParser();
        cmd = null;
        try {
            cmd = parser.parse(options, otherArgs);
        } catch (ParseException e2) {
            // TODO Auto-generated catch block
            e2.printStackTrace();
        }

        setOptions();

        /*
        String mqPassword = null;
        if (mqHadoopCredPath != null && mqPasswordAlias != null) {
            conf.set("hadoop.security.credential.provider.path", mqHadoopCredPath);
            if (mqPasswordAlias != null) {
                SparkCredentialProvider creds = new SparkCredentialProvider();
                char[] pwdChars = creds.getCredentialString(conf.get("hadoop.security.credential.provider.path"),
                        mqPasswordAlias, conf);
                if (pwdChars == null) {
                    System.out.println("Invalid URI for Password Alias or CredPath");
                    System.exit(1);
                }
                mqPassword = String.valueOf(pwdChars);
            }
        }
        */

        System.out.println("Spark Process Streaming Options");
        System.out.println("jobName: "+jobName);
        System.out.println("input: "+input);
        System.out.println("output: "+output);
        System.out.println("SparkBatchDuration: "+batchDuration);

        System.out.println("qmgrHost: "+qmgrHost);
        System.out.println("qmgrPort: "+qmgrPort);
        System.out.println("qmgrName: "+qmgrName);
        System.out.println("qmgrChannel: "+qmgrChannel);
        System.out.println("mqQueue: "+mqQueue);
        System.out.println("mqUsername: "+mqUsername);
        System.out.println("mqWaitTime: "+mqWaitTime);
        System.out.println("mqHadoopCredPath: "+mqHadoopCredPath);
        System.out.println("mqKeepMessages: "+mqKeepMessages);
        System.out.println("mqRateLimit: "+mqRateLimit);


        ReadMQStream mqkafka = new ReadMQStream();
        mqkafka.startStream(jobName, qmgrHost, qmgrPort,
                qmgrName, qmgrChannel, mqQueue, mqUsername, mqPassword, mqWaitTime, mqKeepMessages, batchDuration, mqRateLimit);
    }

    private static void missingParams() {
        String header = "SparkProcessStream Missing Options";
        HelpFormatter formatter = new HelpFormatter();
        String footer = "SparkProcessStream Missing Options";
        formatter.printHelp("get", header, options, footer, true);
        System.exit(0);
    }

    private static void setOptions() {
        if (cmd.hasOption("jobName")) {
            jobName = cmd.getOptionValue("jobName").trim();
        } else {
            System.out.println("Missing JobName");
            missingParams();
            System.exit(0);
        }
        if (cmd.hasOption("input")) {
            input = cmd.getOptionValue("input").trim();
        } else {
            System.out.println("Missing Input Type");
            missingParams();
            System.exit(0);
        }
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output").trim();
        } else {
            System.out.println("Missing Output Type");
            missingParams();
            System.exit(0);
        }

        if (cmd.hasOption("process_class")) {
            processClassName = cmd.getOptionValue("process_class").trim();
        }
        if (cmd.hasOption("mq_keep_messages")) {
            mqKeepMessages = "true";
        }

        if (cmd.hasOption("mq_wait_time")) {
            mqWaitTime = cmd.getOptionValue("mq_wait_time").trim();
        }
        if (cmd.hasOption("batchDuration")) {
            String batchDurationIn = cmd.getOptionValue("batchDuration").trim();
            batchDuration = Long.parseLong(batchDurationIn);
        }

        if (input.equalsIgnoreCase("ibmmq") || output.equalsIgnoreCase("ibmmq")) {
            if (cmd.hasOption("mq_qmgr_host")) {
                qmgrHost = cmd.getOptionValue("mq_qmgr_host").trim();
            } else {
                System.out.println("Missing Qmgr Host");
                missingParams();
                System.exit(0);
            }
            if (cmd.hasOption("mq_qmgr_port")) {
                qmgrPort = cmd.getOptionValue("mq_qmgr_port").trim();
            } else {
                System.out.println("Missing Qmgr Port");
                missingParams();
                System.exit(0);
            }
            if (cmd.hasOption("mq_qmgr_name")) {
                qmgrName = cmd.getOptionValue("mq_qmgr_name").trim();
            } else {
                System.out.println("Missing Qmgr Name");
                missingParams();
                System.exit(0);
            }
            if (cmd.hasOption("mq_qmgr_channel")) {
                qmgrChannel = cmd.getOptionValue("mq_qmgr_channel").trim();
            } else {
                System.out.println("Missing Qmgr Channel");
                missingParams();
                System.exit(0);
            }
            if (cmd.hasOption("mq_queue")) {
                mqQueue = cmd.getOptionValue("mq_queue").trim();
            } else {
                System.out.println("Missing MQ Queue");
                missingParams();
                System.exit(0);
            }
            if (cmd.hasOption("mq_username")) {
                mqUsername = cmd.getOptionValue("mq_username").trim();
            } else {
                System.out.println("Missing MQ Username");
                missingParams();
                System.exit(0);
            }
            if (cmd.hasOption("mq_password")) {
                mqPassword = cmd.getOptionValue("mq_password").trim();
            } else {
                System.out.println("Missing MQ Password");
                missingParams();
                System.exit(0);
            }
            /*
            if (cmd.hasOption("mq_hadoop_cred_path")) {
                mqHadoopCredPath = cmd.getOptionValue("mq_hadoop_cred_path").trim();

            } else {
                System.out.println("Missing MQ Hadoop JCEKS Path");
                missingParams();
                System.exit(0);
            }
            */
            if (cmd.hasOption("mq_max_batch_rate")) {
                mqRateLimit = cmd.getOptionValue("mq_max_batch_rate").trim();
            }
        }
    }
}
