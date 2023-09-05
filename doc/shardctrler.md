<!--
 * @Author: closing
 * @Date: 2023-06-13 14:55:48
 * @LastEditors: closing
 * @LastEditTime: 2023-06-13 15:53:41
 * @Description: 请填写简介
-->
# SharkCtrler
## client
```go
type Clerk struct {
    servers []*labrpc.ClientEnd //多个服务器用于容错
	me     int64 // client id
	leader int   // leader id
	opId   int   // operation id,每发送一个指令就加一，单调递增，用于重复指令检测
}
```
1. Query(num int) Config 向SharkCtrler询问当前Config配置
    1. RPC调用ShardCtrler.Query
    ```go
    type QueryArgs struct{
        Num:      num,//Config Num，SharKCtrler维护一个Config分片，每更新一次Config就把新的Config加进去
        ClientId: ck.me,//Nrand()生成
        OpId:     ck.opId,//operation id,每发送一个指令就加一，单调递增
    }
    type QueryReply struct {
        Err    Err
        Config Config
    }
    ```
2. Join(servers map[int][]string) 向SharkCtrler添加groups
    1. RPC调用ShardCtrler.Join
    ```go
    type JoinArgs struct{
        Servers  map[int][]string // new Gid -> servers mappings
	    ClientId int64
	    OpId     int
    }
    type JoinReply struct {
        Err    Err
    }
    ```
3. Leave(gids []int) 向SharkCtrler移除已离开的groups
    1. RPC调用ShardCtrler.Leave
    ```go
    type LeaveArgs struct {
        GIDs     []int
        ClientId int64
        OpId     int
    }
    type LeaveReply struct {
	Err Err
    }
    ```
4. Move(shard int, gid int) 将shard分片移动到ID为gid的group上
    ```go
    type MoveArgs struct {
        Shard:    shard,
		GID:      gid,
		ClientId: ck.me,
		OpId:     ck.opId,
    }
    type MoveReply struct {
	Err Err
    }
    ```
## server
```go
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
```

1. 给client提供的接口函数
```go
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	reply.Err, reply.Config = sc.commonHandler(Op{Args: *args, ClientId: args.ClientId, OpId: args.OpId})
}

//将op发送给Raft服务器，等待Raft服务器提交该Command(applier函数负责接收)
//将包装成command Entry 写入到commandTbl中，等待result channel返回
//检测raft term变化
commonHandler(op Op) (e Err, r Config){
    ...
    index, term, isLeader := sc.rf.Start(op)
    ...
    c := make(chan applyResult)
	sc.commandTbl[index] = commandEntry{op: op, replyCh: c}
    ...
CheckTermAndWaitReply:

}
``` 

2.  applier 从applyCh中读取applyMsg，将其应用到状态机中（分为Snapshot或Command） 
```go
applier(applyCh, snapshotTrigger, sc.appliedCommandIndex){

//for循环直到applyCh被关闭
for m := range applyCh {
    //如果接受快照，则需要重置下shard controller的状态（commandTbl,appliedCommandIndex)
    if m.SnapshotValid {

        sc.readSnapshot(m.Snapshot)//读取快照
        ···
        //接受快照时，commandTbl中的请求回应WrongLeader让client重试
        for _, ce := range sc.commandTbl {
            ce.replyCh <- applyResult{Err: ErrWrongLeader}
        }
        ...
        continue
    }
    
    //根据上次appliedCommandIndex与这次的间隔判断是否需要生成快照
    ...

    
    op := m.Command.(Op)
    sc.appliedCommandIndex = m.CommandIndex //更新appliedCommandIndex
   
    lastOpResult := sc.ClientTbl[op.ClientId] //重复指令检测
    if op.OpId <= lastOpResult.OpId {
        r = lastOpResult.Result
    }else{//处理命令
        switch op.Args.(type){
            case JoinArgs:
            case LeaveArgs:
            case MoveArgs:
            case QueryArgs:
            default:
				panic(args)
        }

        //将该指令写到ClientTbl中用于下次重复指令检测
        sc.ClientTbl[op.ClientId] = applyResult{OpId: op.OpId, Result: r}
    }
    //删除commandTbl中该指令
    delete(sc.commandTbl, m.CommandIndex)

    //判断该指令并返回结果到replyCh中（在commonHandler函数中等待）
    
    if ce.op.ClientId != op.ClientId || ce.op.OpId != op.OpId {//发送给raft时存储的Op与Raft通过rapplyMsg返回的Op是否相同 //? 什么时候会不同？
        ce.replyCh <- applyResult{Err: ErrWrongLeader}
    } else {
        ce.replyCh <- applyResult{Err: OK, Result: r}
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
```

