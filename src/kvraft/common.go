/*
 * @Author: closing
 * @Date: 2023-04-15 21:17:40
 * @LastEditors: closing
 * @LastEditTime: 2023-06-03 15:36:27
 * @Description: 请填写简介
 */
package kvraft

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrShutdown     = "ErrShutdown"
	ErrInitElection = "ErrInitElection"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64 // id of client
	OpId     int   // client operation id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64 // id of client
	OpId     int   // client operation id
}

type GetReply struct {
	Err   Err
	Value string
}
