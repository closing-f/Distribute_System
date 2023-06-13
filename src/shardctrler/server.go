/*
 * @Author: closing
 * @Date: 2023-04-15 21:17:40
 * @LastEditors: closing
 * @LastEditTime: 2023-06-13 10:31:19
 * @Description: 请填写简介
 */
package shardctrler

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/lablog"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	CheckTermInterval   = 100
	SnapshotMsgInterval = 50
)

type Op struct {
	// Your data here.
	Args interface{}

	ClientId int64
	OpId     int
}

type applyResult struct {
	Err    Err
	Result Config
	OpId   int
}

type commandEntry struct {
	op      Op
	replyCh chan applyResult
}

type ShardCtrler struct {
	mu   sync.Mutex
	me   int
	rf   *raft.Raft
	dead int32 // set by Kill()

	// Your data here.
	commandTbl          map[int]commandEntry // commandIndex -> commandEntry
	appliedCommandIndex int                  // 已经应用的commandIndex

	Configs   []Config              // indexed by config num, config history
	ClientTbl map[int64]applyResult // client id -> last opId

}

// 将op包装成command Entry 写入到commandTbl中，等待result返回
func (sc *ShardCtrler) commonHandler(op Op) (e Err, r Config) {
	if sc.killed() {
		e = ErrShutdown
		return
	}

	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(op)
	if term == 0 {
		sc.mu.Unlock()
		e = ErrInitElection
		return
	}
	if !isLeader {
		sc.mu.Unlock()
		e = ErrWrongLeader
		return
	}
	c := make(chan applyResult)
	sc.commandTbl[index] = commandEntry{op: op, replyCh: c}
	sc.mu.Unlock()

CheckTermAndWaitReply:
	for !sc.killed() {
		select {
		case result, ok := <-c:
			if !ok {
				e = ErrShutdown
				return
			}
			e = result.Err
			r = result.Result
			return
		case <-time.After(CheckTermInterval * time.Millisecond):
			t, _ := sc.rf.GetState()
			if term != t {
				e = ErrWrongLeader
				break CheckTermAndWaitReply
			}
		}
	}

	go func() { <-c }()
	if sc.killed() {
		e = ErrShutdown
	}
	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
	// Your code here.
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	reply.Err, reply.Config = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
	// Your code here.
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	//注册用于RPC通信
	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	applyCh := make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, applyCh)
	sc.commandTbl = make(map[int]commandEntry)
	initConfig := Config{Num: 0}
	for i := range initConfig.Shards {
		initConfig.Shards[i] = 0 //初始状态，所有shard都在0号group
	}
	sc.Configs = []Config{initConfig}
	sc.ClientTbl = make(map[int64]applyResult)

	//TODO
	sc.readSnapshot(persister.ReadSnapshot())

	snapshotTrigger := make(chan bool, 1)

	go sc.applier(applyCh, snapshotTrigger, sc.appliedCommandIndex)

	go sc.snapshoter(persister, snapshotTrigger)

	return sc
}

// 一个group对应一个gid，一个gid对应多个shard
type gidShardPair struct {
	gid    int
	shards []int
}

type gitShardPairList []gidShardPair

// 实现sort.Interface接口,
// TODO 为什么要实现这个接口
func (g gitShardPairList) Len() int {
	return len(g)
}
func (g gitShardPairList) Less(i, j int) bool {
	li, lj := len(g[i].shards), len(g[j].shards)
	return li < lj || (li == lj && g[i].gid < g[j].gid)
}
func (g gitShardPairList) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

// 从一个config当中获取group -> shards的映射
// sortedGroupShards: 按照shard数量从小到大排序的gid -> shards
func (cfg *Config) getGroupShards() (groupLoad map[int][]int, sortedGroupLoad gitShardPairList) {
	groupLoad = make(map[int][]int)
	for shard, gid := range cfg.Shards {
		groupLoad[gid] = append(groupLoad[gid], shard)
	}

	// group that holds no shard
	//对于没有shard的group，也要加入到groupLoad中
	for gid := range cfg.Groups {
		if _, ok := groupLoad[gid]; !ok {
			groupLoad[gid] = []int{}
		}
	}

	sortedGroupLoad = make(gitShardPairList, 0, len(groupLoad))
	for gid, shards := range groupLoad {
		sortedGroupLoad = append(sortedGroupLoad, gidShardPair{gid: gid, shards: shards})
	}
	sort.Sort(sortedGroupLoad)
	return
}

// 根据离开的group和新加入的group，重新分配shard
func (cfg *Config) rebalance(oldConfig *Config, joinGids []int, leaveGids []int) {

	//groupLoad : gid -> shards
	//sortedGroupLoad: 按照shard数量从小到大排序的gidShardPair{gid , shards}
	groupLoad, sortedGroupLoad := cfg.getGroupShards()
	len_oldgroups := len(sortedGroupLoad)

	copy(cfg.Shards[:], oldConfig.Shards[:])

	switch {
	case len(cfg.Groups) == 0:
		//如果没有group，所有shard都在0号group
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}
	case len(leaveGids) > 0:
		//如果有group离开，将离开的group的shard分配给其他group
		leaveGidSet := map[int]bool{} //TODO {}和make的区别
		for _, gid := range leaveGids {
			leaveGidSet[gid] = true
		}
		i := 0

		for _, gid := range leaveGids { //对于要离开的group
			for _, shard := range groupLoad[gid] { //对应要离开group里面的shards
				for leaveGidSet[sortedGroupLoad[i%len_oldgroups].gid] { //找到一个不是要离开的group，从最小负载开始找
					i++
				}
				cfg.Shards[shard] = sortedGroupLoad[i%len_oldgroups].gid //将shard分配给这个group
				i++
			}

		}
	case len(joinGids) > 0:

		len_newgroups := len(cfg.Groups)

		//计算每个新加入的group应该分配多少个shard
		minLoad := NShards / len_newgroups
		nHigherLoadGroups := NShards - minLoad*len_newgroups //需要高负载的group数量
		joinGroupLoad := map[int]int{}

		for i, gid := range joinGids {
			if i >= len_newgroups-nHigherLoadGroups {
				joinGroupLoad[gid] = minLoad + 1
			} else {
				joinGroupLoad[gid] = minLoad
			}

		}

		i := 0
		for _, gid := range joinGids {
			for j := 0; j < joinGroupLoad[gid]; j, i = j+1, i+1 {

				//从最大负载开始找,通过RR的方式分配shard
				idx := (len_oldgroups - 1 - i) % len_oldgroups
				if idx < 0 {
					idx += len_oldgroups
				}
				sourceGroup := &sortedGroupLoad[idx]
				//将高负载group的第一个shard分配给新加入的group
				cfg.Shards[sourceGroup.shards[0]] = gid
				//将分配的shard从高负载group中删除
				sourceGroup.shards = sourceGroup.shards[1:]
			}
		}

	}
}

// 从applyCh中读取applyMsg，将其应用到状态机中
// 修改shard configs
// 通过commandIndex标识的channel将配置更改回复Shard Controller的RPC处理程序
func (sc *ShardCtrler) applier(applyCh <-chan raft.ApplyMsg, snapshotTrigger chan<- bool, lastSnapshoterTriggeredCommandIndex int) {
	var r Config

	for m := range applyCh {
		//如果接受快照，则需要重置下shard controller的状态
		if m.SnapshotValid {
			sc.mu.Lock()
			sc.appliedCommandIndex = m.SnapshotIndex
			sc.readSnapshot(m.Snapshot)

			//接受快照后，回应commandTbl中等待的请求
			//TODO 接受快照时，commandTbl中的请求是否需要回应,回应WrongLeader让client重试
			for _, ce := range sc.commandTbl {
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			}
			sc.commandTbl = make(map[int]commandEntry)
			sc.mu.Unlock()
			continue
		}

		if !m.CommandValid {
			continue
		}

		if m.CommandIndex-lastSnapshoterTriggeredCommandIndex > SnapshotMsgInterval {
			select {
			case snapshotTrigger <- true:
				lastSnapshoterTriggeredCommandIndex = m.CommandIndex
			default:
			}
		}

		op := m.Command.(Op)
		sc.mu.Lock()

		sc.appliedCommandIndex = m.CommandIndex
		lastOpResult := sc.ClientTbl[op.ClientId]
		if op.OpId <= lastOpResult.OpId {
			r = lastOpResult.Result
		} else {
			//TODO Args.(type)的用法，代表什么
			switch args := op.Args.(type) {
			case JoinArgs:
				lastConfig := sc.Configs[len(sc.Configs)-1]
				newConfig := Config{Num: lastConfig.Num + 1, Groups: make(map[int][]string)}
				for gid, servers := range lastConfig.Groups {
					newConfig.Groups[gid] = servers
				}

				joinGids := []int{}
				for gid, servers := range args.Servers {
					newConfig.Groups[gid] = servers
					joinGids = append(joinGids, gid)
				}
				sort.Ints(joinGids)
				newConfig.rebalance(&lastConfig, joinGids, nil)
				sc.Configs = append(sc.Configs, newConfig)
				r = Config{Num: -1} //TODO 为什么要返回-1
			case LeaveArgs:
				lastConfig := sc.Configs[len(sc.Configs)-1]
				// copy from previous config
				newConfig := Config{Num: lastConfig.Num + 1, Groups: make(map[int][]string)}
				for k, v := range lastConfig.Groups {
					newConfig.Groups[k] = v
				}

				for _, gid := range args.GIDs {
					// delete leaving group
					delete(newConfig.Groups, gid)
				}
				// going to rebalance shards across groups
				newConfig.rebalance(&lastConfig, nil, args.GIDs)
				// record new config
				sc.Configs = append(sc.Configs, newConfig)
				r = Config{Num: -1}
			case MoveArgs:
				lastConfig := sc.Configs[len(sc.Configs)-1]
				// copy from previous config
				newConfig := Config{Num: lastConfig.Num + 1, Groups: make(map[int][]string)}
				for k, v := range lastConfig.Groups {
					newConfig.Groups[k] = v
				}
				copy(newConfig.Shards[:], lastConfig.Shards[:])
				// move shard to gid
				newConfig.Shards[args.Shard] = args.GID
				// record new config
				sc.Configs = append(sc.Configs, newConfig)
				r = Config{Num: -1}
			case QueryArgs:
				if args.Num == -1 || args.Num >= len(sc.Configs) {
					r = sc.Configs[len(sc.Configs)-1]
				} else {
					r = sc.Configs[args.Num]
				}
			default:
				panic(args)
			}

			// if r.Num < 0 {
			// 	latestConfig := sc.Configs[len(sc.Configs)-1]
			// 	_, sortedGroupLoad := latestConfig.getGroupShards()
			// }

			sc.ClientTbl[op.ClientId] = applyResult{OpId: op.OpId, Result: r}
		}

		ce, ok := sc.commandTbl[m.CommandIndex]
		if ok {
			delete(sc.commandTbl, m.CommandIndex)
		}
		sc.mu.Unlock()

		if ok {
			if ce.op.ClientId != op.ClientId || ce.op.OpId != op.OpId {
				ce.replyCh <- applyResult{Err: ErrWrongLeader}
			} else {
				ce.replyCh <- applyResult{Err: OK, Result: r}
			}
		}

	}

	//applyCh关闭后，将snapshotTrigger关闭,防止snapshoter阻塞;关闭commandTbl中等待的请求
	close(snapshotTrigger)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, ce := range sc.commandTbl {
		close(ce.replyCh)
	}
}

func (sc *ShardCtrler) snapshoter(persister *raft.Persister, snapshotTrigger <-chan bool) {
	for !sc.killed() {
		// wait for trigger
		_, ok := <-snapshotTrigger

		if ok {
			sc.mu.Lock()
			if data := sc.shardCtrlerSnapshot(); data == nil {
				lablog.ShardDebug(0, sc.me, lablog.Error, "Write snapshot failed")
			} else {
				sc.rf.Snapshot(sc.appliedCommandIndex, data)
			}
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) shardCtrlerSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(sc.Configs) != nil ||
		e.Encode(sc.ClientTbl) != nil {
		return nil
	}
	return w.Bytes()
}

// restore previously persisted snapshot, with mutex held
func (sc *ShardCtrler) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var configs []Config
	var clientTbl map[int64]applyResult
	if d.Decode(&configs) != nil ||
		d.Decode(&clientTbl) != nil {
		lablog.ShardDebug(0, sc.me, lablog.Error, "Read broken snapshot")
		return
	}
	sc.Configs = configs
	sc.ClientTbl = clientTbl
}
