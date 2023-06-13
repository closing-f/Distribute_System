package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	TermCheckInterval      = 100 //ms 检查term是否变化的间隔
	SnapshotCheckInterval  = 100 //ms 检查是否需要snapshot的间隔
	AppliedMsgSnapInterval = 50  //ms 每隔多少ApplyMsg检查是否需要snapshot
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Type  string
	Key   string
	Value string

	//重复指令检测
	ClientId int64
	OpId     int
}

func (op Op) String() string {
	switch op.Type {
	case "G":
		return fmt.Sprintf("{G %s}", op.Key)
	case "P":
		return fmt.Sprintf("{P %s:%s}", op.Key, op.Value)
	case "A":
		return fmt.Sprintf("{A %s:+%s}", op.Key, op.Value)
	default:
		return ""
	}
}

type applyResult struct {
	Err   Err
	Value string
	OpId  int
}

func (r applyResult) String() string {
	switch r.Err {
	case OK:
		if l := len(r.Value); l < 10 {
			return r.Value
		} else {
			return fmt.Sprintf("...%s", r.Value[l-10:])
		}
	default:
		return string(r.Err)
	}
}

type commandEntry struct {
	op      Op
	replyCh chan applyResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate float64 // snapshot if log grows this big

	// Your definitions here.

	commandTbl          map[int]commandEntry // commandIndex -> commandEntry
	appliedCommandIndex int                  // 已经应用的commandIndex

	Tbl       map[string]string     // key -> value
	ClientTbl map[int64]applyResult // clientId -> 最后一次操作的结果

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//检查是否关闭
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{
		Type:     "G",
		Key:      args.Key,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	//在start之前加锁防止raft执行太快，还没来得及为该命令设置replyCh
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)

	//正在选举中
	if term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrInitElection
		return
	}
	//不是leader
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	// lablog.KVDebug(kv.me, lablog.Server, "Start op %v at idx: %d, from %d of client %d", op, index, op.OpId, op.ClientId)

	c := make(chan applyResult) // 为该命令设置replyCh,用在applier goroutine中
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

	// 检查term变化，等待raft执行完毕
CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		//经由raft层的applyCh通道返回给kvserver,然后通过kvserver的applier函数处理,返回该命令执行的结果
		case result, ok := <-c:
			if !ok {
				reply.Err = ErrShutdown
				return
			}
			//执行完毕，返回
			*reply = GetReply{Err: result.Err, Value: result.Value}
			return
		case <-time.After(TermCheckInterval * time.Millisecond):
			//检查term是否变化，变化则退出
			t, _ := kv.rf.GetState()
			if term != t {
				reply.Err = ErrWrongLeader
				break CheckTermAndWaitReply //退出，跳出for循环到下面的go func()
			}
		}
	}

	go func() { <-c }() //防止c阻塞
	if kv.killed() {
		reply.Err = ErrShutdown
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//检查是否关闭
	if kv.killed() {
		reply.Err = ErrShutdown
		return
	}

	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	//在start之前加锁防止raft执行太快，还没来得及为该命令设置replyCh
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)

	//正在选举中
	if term == 0 {
		kv.mu.Unlock()
		reply.Err = ErrInitElection
		return
	}
	//不是leader
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	c := make(chan applyResult) // 为该命令设置replyCh,用在applier goroutine中
	kv.commandTbl[index] = commandEntry{op: op, replyCh: c}
	kv.mu.Unlock()

	// 检查term变化，等待raft执行完毕
CheckTermAndWaitReply:
	for !kv.killed() {
		select {
		//经由raft层的applyCh通道返回给kvserver,然后通过kvserver的applier函数处理,返回该命令执行的结果
		case result, ok := <-c:
			if !ok {
				reply.Err = ErrShutdown
				return
			}
			//执行完毕，返回,对于put和append，返回对错即可
			reply.Err = result.Err
			return
		case <-time.After(TermCheckInterval * time.Millisecond):
			//检查term是否变化，变化则退出
			t, _ := kv.rf.GetState()
			if term != t {
				reply.Err = ErrWrongLeader
				break CheckTermAndWaitReply //退出，跳出for循环到下面的go func()
			}
		}
	}

	go func() { <-c }() //? 为什么要防止c阻塞
	if kv.killed() {
		reply.Err = ErrShutdown
	}

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{}) //rpc函数传递消息时需要注册，否则会报错

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = float64(maxraftstate)

	kv.applyCh = make(chan raft.ApplyMsg) // 用于接收raft层的applyMsg,从而应用到状态机
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.appliedCommandIndex = kv.rf.LastIncludedIndex // 已经应用的commandIndex
	kv.commandTbl = make(map[int]commandEntry)       // commandIndex -> commandEntry
	kv.Tbl = make(map[string]string)                 // key -> value
	kv.ClientTbl = make(map[int64]applyResult)       // clientId -> 最后一次操作的结果

	kv.readSnapshot(persister.ReadSnapshot()) // 读取快照进行初始化

	snapshotTrigger := make(chan bool, 1)

	go kv.applier(kv.applyCh, snapshotTrigger, kv.appliedCommandIndex)

	go kv.snapshoter(persister, snapshotTrigger)

	return kv
}

// 接受来自raft层的applyMsg,并应用到状态机
// 将修改结果答复给KVServer的RPC handler
// 每隔一段时间检查是否需要snapshot
func (kv *KVServer) applier(applyCh chan raft.ApplyMsg, snapshotTrigger chan<- bool, lastSnapshoterTriggeredCommandIndex int) {
	//? 参数名raft.ApplyMsg表示chan的类型

	var r string
	//Go提供了range关键字，将其使用在channel上时，会自动等待channel的动作一直到channel被关闭
	for m := range applyCh {
		//raft发送过来的是快照
		if m.SnapshotValid {

			kv.mu.Lock()
			//将kvserver的状态重置为快照的状态
			kv.appliedCommandIndex = m.SnapshotIndex
			kv.readSnapshot(m.Snapshot)

			// clear all pending reply channel, to avoid goroutine resource leak
			for _, ce := range kv.commandTbl {
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			}
			kv.commandTbl = make(map[int]commandEntry)
			kv.mu.Unlock()
			continue
		}

		if !m.CommandValid {
			continue
		}

		//每隔AppliedMsgSnapInterval个消息，就触发一次快照
		if m.CommandIndex-lastSnapshoterTriggeredCommandIndex > AppliedMsgSnapInterval {
			select {
			case snapshotTrigger <- true:
				lastSnapshoterTriggeredCommandIndex = m.CommandIndex // record as last time triggered commandIndex
			default:
			}
		}
		//发送过来的是命令
		op := m.Command.(Op)
		kv.mu.Lock()
		//将kvserver的已应用的命令的索引更新为raft发送过来的命令的索引
		kv.appliedCommandIndex = m.CommandIndex
		lastOpResult := kv.ClientTbl[op.ClientId]

		//检测是否是重复的命令，如果是重复的命令，就不需要更新kvserver的状态，直接返回之前的结果
		//lastOpResult如果为空时（即第一次执行该命令），lastOpResult.OpId为0，
		//如果opId被初始化从0开始，则会出现第一个opId为0的请求被认为是重复请求的情况，因此opId需要从1开始
		if lastOpResult.OpId >= op.OpId {
			// detect duplicated operation
			// reply with cached result, don't update kv table
			r = lastOpResult.Value
		} else {
			switch op.Type {
			case "G":
				r = kv.Tbl[op.Key]
			case "P":
				kv.Tbl[op.Key] = op.Value
				r = ""
			case "A":
				kv.Tbl[op.Key] = kv.Tbl[op.Key] + op.Value
				r = ""
			}

			// cache operation result
			kv.ClientTbl[op.ClientId] = applyResult{Value: r, OpId: op.OpId}
		}

		//获取命令对应的回复通道
		ce, ok := kv.commandTbl[m.CommandIndex]
		if ok {
			//删除已执行的命令
			delete(kv.commandTbl, m.CommandIndex) // delete won't-use reply channel
		}
		kv.mu.Unlock()

		//返回命令的执行结果
		// only leader server maintains commandTbl, followers just apply kv modification
		if ok {
			lablog.Debug(kv.me, lablog.Server, "Command tbl found for cidx: %d, %v", m.CommandIndex, ce)

			if ce.op != op {
				// Your solution needs to handle a leader that has called Start() for a Clerk's RPC,
				// but loses its leadership before the request is committed to the log.
				// In this case you should arrange for the Clerk to re-send the request to other servers
				// until it finds the new leader.
				//
				// One way to do this is for the server to detect that it has lost leadership,
				// by noticing that a different request has appeared at the index returned by Start()
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			} else {
				ce.replyCh <- applyResult{Err: OK, Value: r}
			}
		}

	}
	// clean all pending RPC handler reply channel, avoid goroutine resource leak
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _, ce := range kv.commandTbl {
		close(ce.replyCh)
	}
}

func (kv *KVServer) snapshoter(persister *raft.Persister, snapshotTrigger <-chan bool) {
	if kv.maxraftstate < 0 {
		// no need to take snapshot
		return
	}

	for !kv.killed() {
		//
		ratio := float64(persister.RaftStateSize()) / kv.maxraftstate
		//生成快照，然后把快照发送给raft
		if ratio > 0.9 {
			// is approaching threshold
			kv.mu.Lock()
			if data := kv.kvServerSnapshot(); data == nil {
				lablog.Debug(kv.me, lablog.Error, "Write snapshot failed")
			} else {
				// take a snapshot
				kv.rf.Snapshot(kv.appliedCommandIndex, data)
			}
			kv.mu.Unlock()

			ratio = 0.0
		}

		select {

		case <-time.After(time.Duration((1-ratio)*SnapshotCheckInterval) * time.Millisecond):
		// waiting for trigger
		case <-snapshotTrigger:
		}
	}
}

// get KVServer instance state to be snapshotted, with mutex held
func (kv *KVServer) kvServerSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.Tbl) != nil ||
		e.Encode(kv.ClientTbl) != nil {
		return nil
	}
	return w.Bytes()
}

// 读取Snapshot,对kv.Tbl和kv.ClientTbl清空
func (kv *KVServer) readSnapshot(data []byte) {
	if len(data) == 0 { // no snapshot data
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tbl map[string]string
	var clientTbl map[int64]applyResult
	if d.Decode(&tbl) != nil ||
		d.Decode(&clientTbl) != nil {
		lablog.Debug(kv.me, lablog.Error, "Read broken snapshot")
		return
	}
	kv.Tbl = tbl
	kv.ClientTbl = clientTbl
}
