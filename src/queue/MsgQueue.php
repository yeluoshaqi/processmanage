<?php
namespace processmanage;

class MsgQueue {

	private $msgQueue;

	public function __construct($flag) {
		$this->getMsgQueue($flag);
	}

	//	获取msg queue 长度
	public function getMsgQueueLen() {
		$stat = $this->getMsgQueueStat();
		return isset($stat['msg_qnum']) ? $stat['msg_qnum'] : 0;
	}

	//	移除消息队列
	public function removeMsgQueue() {
		msg_remove_queue($this->msgQueue);
	}

	public function send($data, $type) {
		msg_send($this->msgQueue, $type, $data, true, true, $errorcode);
		return $errorcode;
	}

	public function receive($type) {
		msg_receive($this->msgQueue, 1, $type, 1024*4, $message, true, MSG_IPC_NOWAIT, $errorcode);
		return ["errorcode" => $errorcode, "msg" => $message];
	}

	// 创建msg queue
	private function getMsgQueue($flag) {
		$path = "/tmp/ftok/{$flag}";
		@mkdir($path);
		$msgKey = ftok($path, 'a');
		$this->msgQueue = msg_get_queue($msgKey, 0666);
		$queueStatus =  $this->getMsgQueueStat();
		var_dump($msgKey, $queueStatus);
	}

	private function getMsgQueueStat() {
		return  msg_stat_queue($this->msgQueue);
	}
}
