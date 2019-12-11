<?php

// require composer autoload file
require(__DIR__ . '/vendor/autoload.php');

// register autoload
spl_autoload_register('autoload');
function autoload($class)
{
    echo "autoload. {$class} \n";
	// $classOrigin = $class;
 //    $classInfo   = explode('\\', $class);
 //    $className   = array_pop($classInfo);
 //    foreach ($classInfo as &$v) {
 //        $v = strtolower($v);
 //    }
 //    unset($v);
 //    array_push($classInfo, $className);
 //    $class       = implode('\\', $classInfo);
 //    $class       = str_replace('naruto', 'src', $class);
 //    $classPath   = __DIR__ . '/'.str_replace('\\', '/', $class) . '.php';
 //    require($classPath);
}

function debug($tag, $info) {

    $info["ts"] = date("y-m-d H:i:s", time());
    if(is_array($info)) {
        $str =  "{$tag}    ------" . var_export($info, true). "\n";
    }else {
        $str = "{$tag}    ------ {$info}";
    }

    file_put_contents("/tmp/naruto/".$info['from'], $str, FILE_APPEND);
}

/* -----------------------demo------------------- */

use Naruto\Manager;

/**
 * example
 * 
 * $config = [
 * 		'passwd' => '123456', // unix user passwd
 * 		'worker_num' => 5, // worker start number
 * 		'hangup_loop_microtime' => 200000, // master&worker hangup loop microtime unit/μs
 * 		'pipe_dir' => '/tmp/', // the directory name of the process's pipe will be storaged
 * ]
 * new Manager($config, $closure)
 */
try {
	new Manager();
} catch (Exception $e) {
	var_dump($e);
}
