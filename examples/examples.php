<?php

require(__DIR__ . '/vendor/autoload.php');

use processmanage\Manager;

[process]
;主进程挂起的时间(纳秒)
hanguploopts=1000
;最大运行次数
maxexecutetimes=1000
startnum=1
minnum=1
maxnum=1
;退出信号发出后最大等待时间
stopwaitmaxts=10
;采样数据长度
samplinglen=60

[log]
logdir="/tmp/processmanage/"

[msgqueue]
;maxlen=20
;singlemsgmaxsize=1024

[queue]
queuename=kafka

[kafka]
topic=test
group=consumer_test1
brokers=127.0.0.1:19091,127.0.0.1:19092,127.0.0.1:19093
autocommitintervalms=100
enableautocommit=true

[redis]



$config = [
    "process" => [
        "hanguploopts" => 1000,
        "maxexecutetimes" => 500,
        "startnum" => 8,
        "minnum" => 4,
        "maxnum" => 16,
        "stopwaitmaxts" => 10,
        "samplinglen" => 60,
    ],
    "log" => [
        "logdir" => "/tmp/processmanage/",
    ],
    "queue" => [
        "queuename" => "kafka",
    ],
    "kafka" => [
        "topic" => "test",
        "group" => "consumer_test1",
        "brokers" => "127.0.0.1:19091,127.0.0.1:19092,127.0.0.1:19093",
        "autocommitintervalms" => 100,
    ]
];
try {
	new Manager($config, function($data) {
		echo "I get data: {$data} \n";
        sleep(1);
	});
} catch (Exception $e) {
	var_dump($e);
}
