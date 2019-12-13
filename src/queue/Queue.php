<?php
namespace processmanage\queue;

abstract class Queue {
	
	//	初始化
	abstract public function init ();

	//	待消费队列长度
	abstract public function length();

	//	获取数据
	abstract public function getData();

	//	关闭
	abstract public function close();
}

