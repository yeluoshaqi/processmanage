<?php
namespace processmanage;

use Closure;

abstract class Process {

	//	标识是master还是worker
	public $type = '';
	public $pid = '';
	public $startTime = 0;

	//	a resource handle that can be used to access the System V message queue.
	protected $processQueue;
	protected $processQueueMaxLen = 20;	//	消息队列最大长度	
	protected $processQueueMsgMaxSize;	//	消息队列单个消息最大长度

	//	用户信号
	protected $userSignal = "";

	//	是否停止进程
	protected  $workerExitFlag = "";

	//	挂起进程时间
	protected static $hangupLoopMicrotime = 200000;

	//	最大运行次数
	protected static $maxExecuteTimes = 500;

	//	当前运行次数
	protected static $currentExecuteTimes = 0;

	
	//	日志目录
	protected static $logdir;

	public function __construct() {
		
		if (empty($this->pid)) {
			$this->pid = posix_getpid();
		}
		$this->startTime = time();
	}

	//	主循环
	abstract protected function hangup();

	//	主循环
	abstract protected function defineSigHandler($signal);

	//	注册信号处理函数
	protected function registerSigHandler() {
		foreach ($this->signalSupport as $v) {
			pcntl_signal($v, [&$this, 'defineSigHandler']);
		}
	}

	//	退出进程
	protected function workerExit() {
		$msg = [
					'from'  => "kill_sign",
					'extra' => $this->pid,
				];
		Process::debug($this->type, $msg);
		$result = posix_kill($this->pid, SIGKILL);		
	}

	//	设置进程名称 
	protected function setProcessName($name = "") {
		 cli_set_process_title($name);
	}

	public static function debug($tag, $info) {

		$info["ts"] = date("y-m-d H:i:s", time());
		if(is_array($info)) {
			$str =  "{$tag}    ------" . var_export($info, true). "\n";
		}else {
			$str = "{$tag}    ------ {$info}";
		}

		file_put_contents(self::$logdir.$info['from'], $str, FILE_APPEND);
	}
}
