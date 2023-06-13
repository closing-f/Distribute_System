/*
 * @Author: closing
 * @Date: 2023-04-15 21:17:40
 * @LastEditors: closing
 * @LastEditTime: 2023-06-03 21:39:40
 * @Description: 请填写简介
 */
package lablog

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Retrieve the verbosity level from an environment variable
// ref: https://blog.josejg.com/debugging-pretty/
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type LogTopic string

const (
	Client  LogTopic = "CLNT"
	Commit  LogTopic = "CMIT"
	Config  LogTopic = "CONF"
	Ctrler  LogTopic = "SCTR"
	Drop    LogTopic = "DROP"
	Error   LogTopic = "ERRO"
	Heart   LogTopic = "HART"
	Info    LogTopic = "INFO"
	Leader  LogTopic = "LEAD"
	Log     LogTopic = "LOG1"
	Log2    LogTopic = "LOG2"
	Persist LogTopic = "PERS"
	Snap    LogTopic = "SNAP"
	Server  LogTopic = "SRVR"
	Term    LogTopic = "TERM"
	Test    LogTopic = "TEST"
	Timer   LogTopic = "TIMR"
	Trace   LogTopic = "TRCE"
	Vote    LogTopic = "VOTE"
	Warn    LogTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	// disable datetime logging
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(serverId int, topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity == 2 && topic != Log && topic != Heart {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if serverId >= 0 {
			prefix += fmt.Sprintf("S%d ", serverId)
		}
		format = prefix + format
		log.Printf(format, a...)
	}

}

func KVDebug(serverId int, topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity == 0 && topic != Log && topic != Heart {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if serverId >= 0 {
			prefix += fmt.Sprintf("S%d ", serverId)
		}
		format = prefix + format
		log.Printf(format, a...)
	}

}

func ShardDebug(gid int, serverId int, topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity == 3 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		if gid >= 0 && serverId >= 0 {
			prefix += fmt.Sprintf("G%d S%d ", gid, serverId)
		}
		log.Printf(prefix+format, a...)
	}
}
