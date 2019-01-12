TH=/sbin:/bin:/usr/sbin:/usr/bin
binDir=`dirname "$0"`

#check to see if the command line parms were passed
if [ $# -lt 1 ] ; then
	echo "Missing Spark Streaming Job Config"
	exit 1
fi

job_config=$1

if [[ ! -f $binDir/jobs/$job_config ]] ; then
	echo "Job Config $job_config does not exist"
	exit 1
fi

. $binDir/jobs/$job_config

EXIT_SCRIPT="false"
if [[ "$jobname" == "" ]] ; then
	echo "job_config is missing jobname variable"
	EXIT_SCRIPT="true"
fi

if [[ "$spark_keytab" == "" ]] ; then
        echo "job_config is missing spark_keytab variable"
        EXIT_SCRIPT="true"
fi

if [[ "$input" == "" ]] ; then
        echo "job_config is missing input variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_qmgr_host" == "" ]] ; then
        echo "job_config is missing mq_qmgr_host variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_qmgr_port" == "" ]] ; then
        echo "job_config is missing mq_qmgr_port variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_qmgr_name" == "" ]] ; then
        echo "job_config is missing mq_qmgr_name variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_qmgr_channel" == "" ]] ; then
        echo "job_config is missing mq_qmgr_channel variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_queue" == "" ]] ; then
        echo "job_config is missing mq_queue variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_password" == "" ]] ; then
        echo "job_config is missing mq_password variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_username" == "" ]] ; then
        echo "job_config is missing mq_username variable"
        EXIT_SCRIPT="true"
fi

if [[ "$mq_keep_messages" == "" ]] ; then
        echo "job_config is missing mq_keep_messages variable"
        EXIT_SCRIPT="true"
fi

if [[ "$batch_duration" == "" ]] ; then
        echo "job_config is missing batch_duration variable"
        EXIT_SCRIPT="true"
fi


if [[ "$EXIT_SCRIPT" == "true" ]] ; then
	exit 1
fi

#Parameters that should not be changed regularly
spark_home=/apps/gss-spark2/current

program_jar_in="$binDir/jars/ibm-mq-hdfs*.jar"
program_jar=`ls -1atr $program_jar_in | tail -1`
spark_streamingmq_jar_in="$binDir/jars/spark-ibm-mq*.jar"
spark_streamingmq_jar=`ls -1atr $spark_streamingmq_jar_in | tail -1`
mq_jar="$binDir/jars/com.ibm.mq.allclient.jar"
json_jar_in="$binDir/jars/json-*.jar"
json_jar=`ls -1atr $json_jar_in | tail -1`


spark_dep_jars="$json_jar,$mq_jar,$spark_streamingmq_jar"

job_class_name="org.apache.spark.streaming.example.SparkProcessStreaming"
spark_executor_heap="2g"
spark_driver_heap="2g"

spark_keytab_princ=`/bin/klist -k $spark_keytab | /bin/tail -1 | /bin/awk '{print $2}' | /bin/tr -d "[:space:]";`
spark_user=`echo $spark_keytab_princ| /bin/awk 'BEGIN {FS="@"} {print $1}';`

echo "Spark Home: $spark_home"
echo "JobName: $jobname"
echo "Spark Keytab: $spark_keytab"

echo "MQ Host: $mq_qmgr_host :: MQ Port: $mq_qmgr_port"
echo "MQ Name: $mq_qmgr_name"
echo "MQ Channel: $mq_qmgr_channel :: MQ Queue: $mq_queue"
echo "MQ Username: $mq_username :: MQ Password Alias: $mq_password_alias"
echo "MQ Keep Messages: $mq_keep_messages"
echo "Program Jar: $program_jar"
echo "Spark Deps Jar: $spark_dep_jars"
echo "Job Class Name: $job_class_name"
echo "Spark Executor Heap: $spark_executor_heap"
echo "Spark Driver Heap: $spark_driver_heap"
echo "Spark Keytab Principal: $spark_keytab_princ"
echo "Spark User: $spark_user"




#Check for Running Job
export KRB5CCNAME=/tmp/$job_name-$username
/bin/kinit -kt $spark_keytab $spark_keytab_princ -c $KRB5CCNAME
YARN_ID=`/bin/yarn application -list -appTypes spark -appStates RUNNING 2> /dev/null| /bin/grep $jobname | /bin/tail -1 | /bin/awk '{print $1}'| /bin/tr -d "[:space:]"`
echo $YARN_ID
if [[ "$YARN_ID" == "" ]] ; then
        /bin/hadoop fs -test -d /user/$spark_user/sparkstreaming/$jobname
        ret=$?
		if [ $ret -ne 0 ] ; then
			/bin/hadoop fs -mkdir -p /user/$spark_user/sparkstreaming/$jobname
		fi		        

	HDPSELECT=`/usr/bin/hdp-select | /usr/bin/grep spark-client | /usr/bin/hdp-select | /usr/bin/grep spark-client | /bin/cut -d " " -f 3`
echo $sparkargs


	spark_gc_args="-XX:+PrintFlagsFinal -XX:+PrintReferenceGC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"

        MQ_PARAMS=`grep "^mq_" $binDir/jobs/$job_config| grep -vi mq_hadoop_cred_path | grep -vi mq_keep_messages;`
        MQPARAMS=""
        for mqparam in $MQ_PARAMS
                do
                 MQPARAM=`echo "--$mqparam" | sed 's;=; ;g' | sed 's;";;g';`
                 MQPARAMS="$MQPARAM $MQPARAMS"
                done

	$spark_home/bin/spark-submit --name $jobname  --master yarn --deploy-mode cluster --principal $spark_keytab_princ --keytab $spark_keytab --files /etc/spark/conf/hive-site.xml#hive-site.xml --conf "spark.serializer=org.apache.spark.serializer.JavaSerializer" --conf "spark.streaming.receiver.blockStoreTimeout=300" --conf "spark.streaming.receiver.writeAheadLog.enable=true" --conf "spark.streaming.driver.writeAheadLog.closeFileAfterWrite=true" --conf "spark.streaming.receiver.writeAheadLog.closeFileAfterWrite=true" --conf "spark.hadoop.yarn.timeline-service.enabled=false" --conf "spark.streaming.concurrentJobs=1" --conf "spark.streaming.receiver.maxRate=1000" --conf "spark.streaming.backpressure.enabled=true" --conf "spark.executor.instances=1" --conf "spark.executor.cores=2" --conf "spark.dynamicAllocation.enabled=false" --conf "spark.cores.max=2" --conf "spark.cleaner.ttl=120" --conf "spark.executor.memory=$spark_executor_heap" --conf "spark.driver.memory=$spark_driver_heap" --conf "spark.driver.extraJavaOptions=-Dhdp.version=$HDPSELECT $spark_gc_args -Dspark.cleaner.ttl=120 -Dspark.cores.max=2" --conf "spark.executor.extraJavaOptions=-Dhdp.version=$HDPSELECT -Dhdfs.writeahead.tmpdir=/tmp $spark_gc_args" --jars $spark_dep_jars --class $job_class_name $program_jar --jobName $jobname --input $input --output $output --batchDuration $batch_duration $MQPARAMS &

else
        echo "Spark Job $jobname is already running"
fi

