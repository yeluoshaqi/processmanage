<?php

namespace processmanage;

use processmanage\WorkerProducter;
use processmanage\WorkerConsumer;
use processmanage\queue\MsgQueue;
use Exception;

date_default_timezone_set('Asia/Shanghai');

/**
 * process manager
 */
class Manager extends Process {

	//	wokers pool
	public  $workers = [];
	private $startNum = 8;
	private $minNum = 4;
	private $maxNum = 16;

	//	扩缩容策略采样数据容量
	private $samplingBucket =[]
	private $samplinglen =60;
	private $extendAndStrategyCapacityTime=0;
	
	private $signalSupport = [
		'reload'    => SIGUSR1,
		'stop'	    => SIGUSR2,
		'terminate' => SIGTERM,
		'int'		=> SIGINT
	];

	//	接受退出信号后程序最大等待时间
	private $signalStopTs = 0;
	private $stopWaitMaxTs = 10;
	
	
	public function __construct() {

		$this->type = "manager";
		
		$this->loadConfig();

		parent::__construct();
		//	获取消息队列
		$this->msgQueue = new MsgQueue($this->pid);

		// exectue fork
		$this->forkProduct();
		$this->execForkConsumer();

		// register signal handler
		$this->registerSigHandler();

		$this->extendAndStrategyCapacityTime = time();

		// hangup master
		$this->hangup();
	}

	public function __get($name = '') {
		return $this->$name;
	}

	public function loadConfig() {

		$config = parse_ini_file("../config/config.ini", true);

		$this->startNum = (int)$config['startnum'] ? $this->startNum : 8;
		$this->minNum = (int)$config['minnum'] ? $this->minNum : 4;
		$this->maxNum = (int)$config['maxnum'] ? $this->maxNum : 16;
		$this->stopWaitMaxTs = (int)$config['stopwaitmaxts'] ? $this->stopWaitMaxTs : 10;
		$this->samplingLen = (int)$config['samplinglen'] ? $this->samplingLen : 60;
		$this->logdir = $config['logdir'] ? $config['logdir'] : '/tmp';

		self::$hangupLoopMicrotime = $config['hanguploopts'] ? self::$hangupLoopMicrotime : 500;
		self::$maxExecuteTimes = $config['maxexecutetimes'] ? self::$maxExecuteTimes : 500;
	}

	public function execForkConsumer($num = 0) {
		foreach (range(1, $num? : $this->startNum) as $v) {
			$this->forkConsumer();
		}
	}
	
	private function forkConsumer() {

		$pid = pcntl_fork();
		switch ($pid) {
			case -1:
				// exception
				exit;
				break;
			
			//	子进程
			case 0:
				try {
					$worker = new WorkerConsumer([
						'type' => "consumer_workers",
						'msg_queue' => $this->msgQueue,
					]);
					$worker->hangup();
				} catch (Exception $e) {
					var_dump($e);
				}
				exit(0);

			//	主进程
			default:
				try {
					$worker = new WorkerConsumer([
							'type' => 'master_consumer_workers',
							'pid'  => $pid
						]);
					$this->workers['consumer'][$pid] = $worker;
				} catch (Exception $e) {
					var_dump($e);
				}
				break;
		}
	}
	private function forkProduct() {

		$pid = pcntl_fork();
		switch ($pid) {
			case -1:
				exit;
				break;
			
			//	子进程
			case 0:
				try {
					$worker = new WorkerProducter([
						'type' => "producter_workers_".$this->pid,
						'msg_queue' => $this->msgQueue,
					]);
					$worker->hangup();
				} catch (Exception $e) {
					var_dump($e);
				}
				exit;

			//	主进程
			default:
				try {
					$this->workers['producter'][$pid] = new WorkerProducter([
						'type' => 'master_producter_workers',
						'pid'  => $pid
					]);
				} catch (Exception $e) {
					var_dump($e);
				}
				break;
		}
	}

	//	主循环
	protected function hangup() {

		while (true) {
			
			//	信号分发
			pcntl_signal_dispatch();

			//	检查product子进程状态
			foreach ($this->workers['producter'] as $pid => $v) {
				$res = pcntl_waitpid($pid, $status, WNOHANG);	//	wnohange 非阻塞
				if($res > 0 || $res  == -1) {
					// echo " producter {$pid}: status {$status}   res: {$res} userSignal {$this->userSignal}\n";
					unset($this->workers['producter'][$res]);
					if($this->userSignal === 'stop') {
						echo "hangup producter pid:{$pid}  stop. \n";
					}
					if ($this->userSignal === 'reload' || $this->userSignal === "" || $this->userSignal === 'shrinkcapacity') {
						echo "hangup producter pid:{$pid}  stop. signal:{$this->userSignal}  fork\nn";
						$this->forkProduct();
					}
				}
			}

			//检查consumer子进程状态	
			foreach ($this->workers['consumer'] as $pid => $v) {
				//	如果有子进程退出 $res 为子进程pid
				$res = pcntl_waitpid($pid, $status, WNOHANG);
				// echo " consumer {$pid}: status {$status}   res: {$res}\n";
				if ($res > 0 || $res  == -1) {
					
					unset($this->workers['consumer'][$res]);
					echo "hangup pcntl_waitpid:{$pid}   status:{$status}  res:{$res}\n";

					//	重启(重启) 子进程超过运行次数正常重启
					if ($this->userSignal === 'reload' || $this->userSignal === "") {
						echo "hangup consumer pid:{$pid}   stop.  signal:{$this->userSignal}  fork\n";
						$this->forkConsumer();
					}elseif ($this->userSignal === 'shrinkcapacity' && count($this->workers['consumer']) < $this->minNum) {
						echo "hangup consumer pid:{$pid}   stop.  signal:{$this->userSignal}  fork\n";
						$this->forkConsumer();
					}elseif ($this->userSignal === 'stop') {
						echo "hangup consumer pid:{$pid}   stop. \n";
					}
				}
			}

			if ($this->userSignal === 'stop') {
				
				if (empty($this->workers['consumer']) && empty($this->workers['producter'])) {
					$this->workerExit();
				}

				//	强制退出
				if($this->signalStopTs && $this->signalStopTs + $this->stopWaitMaxTs < time() ) {
					$this->killWorkerForce();
				}
			}

			// 扩容策略
			$this->capacityStrategy();

			sleep(1);
		}
	}

	protected function workerExit() {
		echo "{$this->type} workerExit . queuelen: ". $this->getMsgQueueLen() . "\n";
		$this->msgQueue->removeMsgQueue();
		parent::workerExit();
	}

	//	信号处理
	public function defineSigHandler($signal = 0) {
		
		echo "signal {$signal} \n";
		switch ($signal) {
			
			case $this->signalSupport['reload']:
				
				$this->userSignal = "reload";
				foreach ($this->workers['consumer'] as $v) {
					posix_kill($v->pid, SIGUSR1);
					echo "defineSigHandler posix_kill consumer:{$v->pid} signal:{SIGUSR1} \n";
				}
				break;

			case $this->signalSupport['terminate']:
			case $this->signalSupport['int']:
			case $this->signalSupport['stop']:
			case $this->signalSupport['kill']:

				$this->userSignal = "stop";
				foreach ($this->workers['producter'] as $v) {
					posix_kill($v->pid, SIGUSR2);
					echo "defineSigHandler posix_kill producter:{$v->pid} signal:{SIGUSR2} \n";
				}
				foreach ($this->workers['consumer'] as $v) {
					posix_kill($v->pid, SIGUSR2);
					echo "defineSigHandler posix_kill consumer:{$v->pid} signal:{SIGUSR2} \n";
				}
				$this->signalStopTs = time();
				break;
			default:
				break;
		}
	}

	//	扩容策略
	private function capacityStrategy() {
		
		$now = time();
		$data = $this->msgQueue->receive(2);

		if($data['errorcode'] > 0) {
			//	3分钟内 没有发生扩缩容时间的变化 kafka连接可能出现连接问题
			if($this->extendAndStrategyCapacityTime + 180 < $now) {
				$this->extendAndStrategyCapacityTime = $now;
				$this->samplingBucket = [];
				$this->shrinkcapacity();
			}
			return;
		}
		$this->samplingBucket[] = $data['msg']["length"];

		$count = count($this->samplingBucket);
		if($count < $this->samplinglen) {
			return ;
		}

		array_shift($this->samplingBucket);

		if($this->extendAndStrategyCapacityTime + 60 > $now) {
			return ;
		}

		$this->extendAndStrategyCapacityTime = $now;

		$l_100 = $l_1000 = $l_10000 = $l_3000 = $l_5000 = 0;
		foreach ($this->samplingBucket as $key => $value) {
			if($value > 1000) {
				$l_1000 ++;
			}
			if($value > 3000) {
				$l_3000 ++;
			}
			if($value > 5000) {
				$l_5000 ++;
			}
			if($value > 10000) {
				$l_10000 ++;
			}
			if($value< 50) {
				$l_100 ++;	
			}
		}

		//	1000条  20进程(每条100ms)  5s处理完成 
		$threshold = count($this->samplingBucket) * 3 / 4;
		//	 扩缩容策略
		$strategy = 0;
		if($l_10000 > $threshold) {
			$strategy = 2;
		}elseif($l_5000 > $threshold){
			$strategy = 1.5;
		} elseif($l_3000 > $threshold){
			$strategy = 1;
		} elseif($l_1000 > $threshold){
			$strategy = 0.5;
		} elseif($l_100 == count($this->samplingBucket)) {
			$strategy = -1;
		}

		echo "扩缩容策略 {$strategy} \n";

		if($strategy == 0) {
			return ;
		}elseif($strategy > 0) {
			$this->extendCapacity($strategy);
		}else if($strategy < 0) {
			$this->shrinkCapacity();
		}
	}

	//	缩容
	private function shrinkCapacity() {
		$this->userSignal = 'shrinkcapacity';
		$workerList = [];
		foreach ($this->workers['consumer'] as $pid => $worker) {
			$workerList[$pid] = $worker->startTime;
		}
		
		arsort($workerList);
		$workerNum = count($workerList);
		//	缩容的数量 减少当前工作进程的一半但不能比mixNUm少
		$shrinkLen = ($workerNum/2 > $this->minNum) ? $workerNum /2 : $workerNum - $this->minNum;
		$workerList = array_keys($workerList);

		while ($shrinkLen -- > 0) {
			$pid = array_pop($workerList);
			posix_kill($pid, SIGUSR2);
			echo "shrink capacity posix_kill {$pid}   ";
		}
		echo "\n this worker number " . count($this->workers['consumer']) . " \n\n";
	}

	//	扩容
	private function extendCapacity($strategy) {
		
		$this->userSignal = "";
		//	增加的进程数目
		$process = count($this->workers['consumer']) * $strategy;
		
		//	系统负载不高的情况下fork
		$sysLoadAvg	= sys_getloadavg();
		echo "\n sysLoadAvg:" . json_encode($sysLoadAvg) ."\n";
		$cupCores = $this->getCpuCores();
		if($sysLoadAvg[0] < 2 * $cupCores || $sysLoadAvg[1] < 2 * $cupCores || $sysLoadAvg[2] < 2 * $cupCores) {
			while ($process-- > 0) {
				if(count($this->workers['consumer']) >= $this->maxNum) {
					break;
				}
				$this->forkConsumer();
				echo "forkConsumer, workers number: " . count($this->workers['consumer']) . " process {$process}\n";
			}
		}
		echo "this worker number " . count($this->workers['consumer']) . " \n\n";
	}

	// 强制性停止worker
	private function killWorkerForce() {
		foreach ($this->workers['consumer'] as $pid => $value) {
			posix_kill($pid, 9);
			echo "killWorkerForce posix_kill consumer:{$pid} signal:9 \n";
		}
		foreach ($this->workers['producter'] as $pid => $value) {
			posix_kill($pid, 9);
			echo "killWorkerForce posix_kill producter:{$pid} signal:9 \n";
		}
	}

	private function getCpuCores() {

		return 4;
		$result = exec('cat /proc/cpuinfo |grep "cpu cores"|uniq', $result, $status);
		echo "cpu cores: {$result} \n";
		$arr = explode(':', $result);
		return trim($arr[1]);
	}
} 
