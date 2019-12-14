<?php
namespace processmanage\queue;

use Exception;
use processmanage\process;

class Redis extends Queue{
	
	private $host;
	private $port;
	private $key;
	private $redis;

	public function __construct($config = []) {
		$this->host = isset($config['host']) ? $config['host'] : "";
		$this->port = isset($config['port']) ? $config['port'] : "";
		$this->key = isset($config['key']) ? $config['key'] : "";
		if(empty($this->host) || empty($this->port)) {
			throw new Exception("host port is empty", 1);
		}
	}

	public function init () {
		$this->redis = new \Redis();
		$this->redis->connect($this->host, $this->port);
	}

	//	待消费队列长度
	public function length() {

		return $this->redis->llen($this->key);
	} 

	public function getData() {

		$str = $this->redis->brpoplpush($this->key, $this->key."_test", 1);
		
		if(empty($str)) {
			$str = "";
		}

		return $str;
	}

	public function close() {
		$this->redis->close();
	}
}

