<?php

namespace processmanage;

use Closure;
use processmanage\Process;
use processmanage\queue\Kafka;
use processmanage\queue\ProcessQueue;

class WorkerProducter extends Process {

	//	发动未消费队列长度的频率 每3秒一次
	public $frequency = 3;
	public $sendQueueLengthTime;
	public $emptyDataTime;

	protected $signalSupport = [
      	'stop'      => SIGUSR2, // quit signal gracefully stop
	];

	//	消息源配置
	private $queueName;
	private $queueConfig;


	public function __construct($config = []) {
		$this->type    = isset($config['type'])? $config['type']: 'producter_workers';
		$this->processQueue    = isset($config['process_queue'])? $config['process_queue']: '';
		$this->pid    = isset($config['pid'])? $config['pid']: '';

		parent::__construct();
	}

	//	信号处理
	public function defineSigHandler($signal = 0) {
		
		$msg = ['from'  => "producter_sign", 'extra' => ["signal" => $signal, "pid" => $this->pid]];
		Process::debug("get signal ", $msg);

		switch ($signal) {
			//	stop
			case SIGUSR2:
				$this->workerExitFlag = "stop";
				break;

			default:
				break;
		}
	}

	//	主循环
	public function hangup() {

		$this->loadConfig();

		$name = $this->queueName;
		// $this->queue = new $name($this->queueConfig);
		$this->queue = new Kafka($this->queueConfig);

		$this->queue->init();

		$this->registerSigHandler();

		$this->emptyDataTime = time();

		while (true) {
			
			//等待消费的队列长度
			$this->forConsumptionLength();

			//	进程间msg队列过长 
			if($this->processQueue->length() >= $this->processQueueMaxLen) {
				$msg = ['from'  => $this->type,'extra' => " ------- msg queue len to large: ". $this->processQueue->length()];
				Process::debug("msg queue len to large ", $msg);
				usleep(1000);
				continue;
			}

			// work
			$this->work();

			//	信号处理
			pcntl_signal_dispatch();
			if ($this->workerExitFlag === "stop") {
				$this->workerExit();
			}
		}
	}

	// 关闭进程
	protected function workerExit() {

		$this->queue->close();
		parent::workerExit();
	}

	private function work() {

		//	获取数据
		try{
			$str = $this->queue->getData();
		} catch(Exception $e) {
			var_dump($e);
			$this->workerExit();
		}
		
		//	如果数据为空
		if(empty($str)) {
			$this->emptyDataTime = time();
			usleep(1000);
			return ;
		}
		
		//	发送数据
		$this->processQueue->send($str, 1);
	}

	//	等待消费的队列长度
	private function forConsumptionLength() {
		
		//	发送未消费队列的长度
		$now = time();
		if($now % $this->frequency == 0 && $this->sendQueueLengthTime != $now) {
		
			//	当前时间和空数据时间差值大于10S
			if($now - $this->emptyDataTime > 10) {
				try{
					$queueLength = $this->queue->length();
				}catch(Exception $e) {
					var_dump($e);
					$this->workerExit();
				}
			} else {
				$queueLength = 0;
			}

			$this->processQueue->send(['ts' => $now, 'length' => $queueLength], 2);

			$this->sendQueueLengthTime = $now;
		}
	}

	private function loadConfig() {
		
		$config = parse_ini_file(dirname(__FILE__) . "/config/config.ini", true);

		$queuename = $config['queue']['queuename'] ? $config['queue']['queuename'] : 'kafka';
		$this->queueConfig = $config[$queuename] ? $config[$queuename] : [];
		$this->queueName = ucfirst($queuename);
	}
}

