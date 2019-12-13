<?php
namespace processmanage\queue;

class ProcessQueue {

	private $processQueue;

	public function __construct($flag) {
		$this->getQueue($flag);
	}

	//	获取msg queue 长度
	public function length() {
		$stat = $this->getStat();
		return isset($stat['msg_qnum']) ? $stat['msg_qnum'] : 0;
	}

	//	移除消息队列
	public function remove() {
		msg_remove_queue($this->processQueue);
	}

	public function send($data, $type) {
		msg_send($this->processQueue, $type, $data, true, true, $errorcode);
		return $errorcode;
	}

	public function receive($type) {
		msg_receive($this->processQueue, $type, $msgType, 1024*4, $message, true, MSG_IPC_NOWAIT, $errorcode);
		return ["errorcode" => $errorcode, "msg" => $message, "type" => $msgType];
	}

	// 创建msg queue
	private function getQueue($flag) {
		$path = "/tmp/ftok/{$flag}";
		@mkdir($path);
		$msgKey = ftok($path, 'a');
		$this->processQueue = msg_get_queue($msgKey, 0666);
		$queueStatus =  $this->getStat();
		var_dump($msgKey, $queueStatus);
	}

	private function getStat() {
		return  msg_stat_queue($this->processQueue);
	}
}
