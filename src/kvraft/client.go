package kvraft

import (
	"time"

	"6.824/labrpc"
	"6.824/labutil"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.、
	me     int64 // my client id
	leader int   // leader id
	opId   int   // operation id

}

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = labutil.Nrand()
	ck.leader = 0
	ck.opId = 1 //初始opId为0时，会出现第一个opId为0的请求被认为是重复请求的情况
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key:      key,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader
	for {
		reply := &GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", args, reply)
		//当!ok或不是当前leader或leader关掉时，遍历访问下一个server
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		//如果是ErrInitElection，则等待100ms后重试
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId //更新leader信息
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			return reply.Value
		}

	} //for
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader
	for {
		reply := &PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = serverId
		if reply.Err == OK {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "P")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "A")
}
