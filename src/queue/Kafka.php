<?php
namespace processmanage\queue;

class Kafka extends Queue{
	
	public static $partitions = [];
	
	private $topic ;
	private $group ;
	private $brokers ;
	private $autoCommitIntervalMs ;
	private $enableAutoCommit;
	private $kfkConsumer;

	public function __construct($config = []) {
		$this->topic = isset($config['topic']) ? $config['topic'] : "test";
		$this->group = isset($config['group']) ? $config['group'] : "consumer_test1";
		$this->brokers = isset($config['brokers']) ? $config['brokers'] : "127.0.0.1:19091,127.0.0.1:19092,127.0.0.1:19093";
		$this->autoCommitIntervalMs = isset($config['autoCommitIntervalMs']) ? $config['autoCommitIntervalMs'] : 100;
		$this->enableAutoCommit = isset($config['enableAutoCommit']) ? $config['enableAutoCommit'] : true;
	}

	public function init () {
		$conf = new \RdKafka\Conf();
		$conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
		    Kafka::$partitions = [];
		    switch ($err) {
		        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		        	foreach ($partitions as $value) {
		        		$partition = $value->getPartition();
		        		Kafka::$partitions[$partition] = $partition;
		        	}
		           	var_dump("consumer assign partition", $partitions);
		            $kafka->assign($partitions);
		            break;

		        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
		           	foreach ($partitions as $value) {      
		        		$partition = $value->getPartition();
		        		Kafka::$partitions[$partition] = $partition;
		        	}
		           	var_dump("consumer revoke partitions", $partitions);
		            $kafka->assign(NULL);
		            break;

		        default:
		            throw new \Exception($err);
		    }
		});

		$conf->setOffsetCommitCb(function(\RdKafka\KafkaConsumer $kafka, $err, $partitions) {
			if($err > 0) {
		   		printf(" setOffsetCommitCb Kafka error: %d  :  %s (partitions: %s)\n", $err, rd_kafka_err2str($err), json_encode($partitions));
			}
		});

		$conf->setErrorCb(function(\RdKafka\KafkaConsumer $kafka, $err, $reason) {
			if($err > 0) {
		    	printf("setErrorCb Kafka error: %d  :  %s (reason: %s)\n", $err, rd_kafka_err2str($err), $reason);
			}
		});

		$conf->setConsumeCb(function ($message) {
		    var_dump("message", $message);
		});

		$conf->set('group.id', $this->group);
		$conf->set('metadata.broker.list', $this->brokers);
		$conf->set('enable.auto.commit', $this->enableAutoCommit);
		$conf->set('auto.commit.interval.ms', $this->autoCommitIntervalMs);
	
		$this->kfkConsumer = new \RdKafka\KafkaConsumer($conf);
		$this->kfkConsumer->subscribe([$this->topic]);
	}

	//	待消费队列长度
	public function length() {

		if(count(self::$partitions) == 0) {
			return 0;
		}
        
        $topicPartitionObjs = array();
		foreach (self::$partitions as $partition) {
        	$this->kfkConsumer->queryWatermarkOffsets($this->topic, $partition, $old, $new, 3000);
			$newoffsets[$partition] = $new;
			if(!isset($this->topicPartitionObjs[$partition])) {
				$this->topicPartitionObjs[$partition] = new \RdKafka\TopicPartition($this->topic, $partition);
			}
			$topicPartitionObjs[] = $this->topicPartitionObjs[$partition];
		}

        $currentTopicPartition = $this->kfkConsumer->getCommittedOffsets($topicPartitionObjs, 1000);

        $debugInfo = '';
        $lens = 0;
        foreach ($currentTopicPartition as $value) {
        	$partition = $value->getPartition();
        	$currentOffset = $value->getOffset();
        	$lens += $newoffsets[$partition] - $currentOffset;
        	$debugInfo .= "pt:{$partition} new:{$newoffsets[$partition]} current:{$currentOffset}    ";
        }
        $debugInfo .=  "  len {$lens}";

        debug("partitionLengh", ["from" => "kafka", "extra" => $debugInfo]);
        
        return $lens;
	} 

	public function getData() {

		$str = "";
	    $message = $this->kfkConsumer->consume(5*1000);
	    switch ($message->err) {
	        case RD_KAFKA_RESP_ERR_NO_ERROR:
        		// echo $message->partition . "   " . $message->payload . "\n";
				$str = $message->payload;
	            break;
	        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
	        	echo "partition end \n";
	            break;
	        case RD_KAFKA_RESP_ERR__TIMED_OUT:
	        	echo "time out \n";
	            break;
	        default:
		        echo $message->errstr() . " -------- +++++++ \n";
	            throw new \Exception($message->errstr(), $message->err);
	            break;
	    }
		return $str;
	}

	public function close() {
		$this->kfkConsumer->close();
	}
}

