<?php

namespace processmanage;

use processmanage\Process;
use processmanage\queue\MsgQueue;

class WorkerConsumer extends Process {
		
	protected $signalSupport = [
	   	'reload'   => SIGUSR1, // reload signal
      	'stop'      => SIGUSR2, // quit signal gracefully stop
	];


	public function __construct($config = []) {
		$this->type    = isset($config['type'])? $config['type']: 'WorkerConsumer';
		$this->msgQueue    = isset($config['msg_queue'])? $config['msg_queue']: '';
		$this->pid    = isset($config['pid'])? $config['pid']: '';

		parent::__construct();
	}

	//	主循环
	public function hangup() {

		$this->registerSigHandler();

		while (true) {
			//	do work
			$this->deal();

			//	信号分发
			pcntl_signal_dispatch();

			// 如果进程是待退出状态 并且msg queue已处理完毕
			if ($this->workerExitFlag === "stop" && $this->msgQueue->getMsgQueueLen() == 0) {
				$this->workerExit();
				return;
			}
			//	如果进程是重启状态
			if ($this->workerExitFlag === "reload") {
				$this->workerExit();
				return;
			}

			// 并且当前运行次数到达最大运行次数 并且 如果进程是非停止状态
			if (self::$currentExecuteTimes >= self::$maxExecuteTimes && $this->workerExitFlag === "") {
				$this->workerExit();
				return;
			}
			
			//	当前运行次数+1
			++self::$currentExecuteTimes;
		}
	}

	//	信号处理
	public function defineSigHandler($signal = 0) {
		
		$msg = ['from'  => "consumer_sign", 'extra' => ["signal" => $signal, "pid" => $this->pid]];
		Process::debug("get signal ", $msg);

		switch ($signal) {
			//	reload
			case SIGUSR1:	
				$this->workerExitFlag = "reload";
				break;
			//	stop
			case SIGUSR2:
				$this->workerExitFlag = "stop";
				break;

			default:
				break;
		}
	}

	private function deal() {
		$time = microtime(true);
		$data = $this->msgQueue->receive(1);
		$time = microtime(true) - $time;
		$strlen = strlen($data['msg']);
		$queuelen = $this->msgQueue->getMsgQueueLen();

		$msg = ['from'  => "msg_receive",	'extra' => "msg receive res: {$data['msg']}, len:{$strlen}, messageType:1,  time:{$time},  errorcode:{$data['errorcode']}, queuelen:{$queuelen}",];
		Process::debug("msg receive ", $msg);

		if(empty($message)) {
			sleep(1);
		}
		// sleep(1);
		usleep(20000);
	}
}