[TOC]

# Raft服务器初始化

```go
rf := &Raft{}//详细配置省略	

//读取持久化保存的内容
rf.readPersist(persister.ReadRaftState())

//定时器超时处理，follower超时时开始竞选leader
go rf.ticker()

//根据commitIndex的变化，将日志应用到状态机上
go rf.commiter(applyCh, rf.CommitTrigger)

// 根据情况保存传过来的快照
go rf.snapshoter(rf.SnapshotTrigger)
```

## Ticker()

### ticker()函数中, 当follower定时器超时时的操作

ticker()  1.自己state改变

```go
rf.CurrentTerm++ //? 如果竞选失败，还会再加回来吗？
term := rf.CurrentTerm
rf.VotedFor = rf.me
rf.State = CANDIDATE

rf.persist()

rf.ElectionAlarm = time.Now().Add(time.Duration(rand.Intn(150)+150) * time.Millisecond)

sleepDuration = time.Until(rf.ElectionAlarm)
```

ticker()  2.向各个server发送RequestVote，并收集投票

```go
grant := make(chan bool)
// var reply RequestVoteReply
for i := 0; i < len(rf.peers); i++ {
    if i != rf.me {
        go rf.pre_sendRequestVote(term, i, args, grant)
    }
}

go rf.collectVote(grant)
```

#### 1. ticker()->pre_sendRequestVote()

```go
rf.mu.Lock()
//判断是否已经成为leader(可能已经获得多数投票);自己的term发生变化
if rf.State != CANDIDATE || rf.killed() || term != rf.CurrentTerm {
    rf.mu.Unlock()
    return
}
rf.mu.Unlock()
reply := &RequestVoteReply{}
ret := rf.sendRequestVote(server, args, reply)//调用Call发送给各服务器
```

根据reply返回值判断

```go
//答复的Term比自己的还大，转为follower
if reply.Term > rf.CurrentTerm {
	rf.toFollower(reply.Term)
	return
}
granted = reply.VoteGranted
```

#### 2. ticker()->collectVote() 收集投票，投票超过一半，竞选为leader

投票超过一半时

```go
rf.mu.Lock()
//先进行三大判断
if rf.State != CANDIDATE || rf.CurrentTerm != term || rf.killed() {
    rf.mu.Unlock()
} else {
    
    //成为leader
    rf.State = LEADER
    rf.NextIndex = make([]int, len(rf.peers))
    rf.MatchIndex = make([]int, len(rf.peers))

    for i := 0; i < len(rf.peers); i++ {
        rf.NextIndex[i] = len(rf.Log) //?为什么要加1,论文规定
        rf.MatchIndex[i] = 0          // safe to initialize to 0
    }
	
    //对每个follower发送AppendEntries,为每个server启动goroutine用于发送AE
    rf.AppendEntriesChan = make([]chan int, len(rf.peers))
    for i := 0; i < len(rf.peers); i++ {
        rf.AppendEntriesChan[i] = make(chan int)
        go rf.entriesAppender(i, rf.AppendEntriesChan[i], rf.CurrentTerm)
     
    }
    
    //为每个server启动goroutine用于发送IS
    rf.InstallSnapshotChan = make([]chan int, len(rf.peers))
    for i := 0; i < len(rf.peers); i++ {
        if i != rf.me {
            rf.InstallSnapshotChan[i] = make(chan int, 1)
            go rf.snapshotInstaller(i, rf.InstallSnapshotChan[i], 						rf.CurrentTerm)
        }
    }
    //每隔一段时间发送心跳包
    go rf.leaderTicker(rf.CurrentTerm)

    rf.mu.Unlock()

}

```

##### ·  ticker()->collectVote()->leaderTicker() 按间隔向follower发送心跳包

```go
for !rf.killed() {
    rf.mu.Lock()
	
    //如果自己不是leader或term变化则退出
    if rf.State != LEADER || rf.CurrentTerm != term {
        rf.mu.Unlock()
        return
    }

    //每隔heartInterval通过chan提醒发送心跳包
    for i, c := range rf.AppendEntriesChan {
        if i != rf.me {
            select {
                case c <- 0:
            default:
                }
        }

    }
    rf.mu.Unlock()

    time.Sleep(heartInterval * time.Millisecond)

}
```

##### ·  ticker()->collectVote()->entriesAppender()  为每个server维持一个goroutine根据条件不断发送AE直到不再是leader

```go
i := 1 //用于记录发送的次数

for !rf.killed() {
    //等待新的AppendEntries RPC
    serialNo, ok := <-ch
    if !ok {
        return //如果channel已经关闭，那么就退出
    }
    rf.mu.Lock()

    //如果term发生变化，或者不是Leader 那么就不用发送了
    if rf.State != LEADER || rf.CurrentTerm != term || rf.killed() {
        rf.mu.Unlock()
        return
    }
    args := rf.constructAppenderArgs(server)

    rf.mu.Unlock()
	//=0代表心跳包 >=i代表重发
    if serialNo == 0 || serialNo >= i {
        go rf.appendEntries(server, args, term, i)
        i++ //发送次数加1
    }

}
```

###### ·  ticker()->collectVote()->entriesAppender()->appendEntries() 根据发送AE的RPC返回结果进行各种判断

1. sendAppendEntries会调用Call函数向peers发送AE请求

```go
reply := &AppendEntriesReply{}
ret := rf.sendAppendEntries(server, args, reply)

rf.mu.Lock()
defer rf.mu.Unlock()
//三大判断
if rf.State != LEADER || rf.CurrentTerm != term || rf.killed() {
return
}
```

2. 发送AE失败情况判断

```go
if !ret {
    //心跳包直接忽视
    if len(args.Entries) == 0 {
        
        return
    }
    //一开始给args.PrevlogIndex赋值时，是rf.nextIndex[server]-1，这里判断是不是小于这个值，如果是，说明这个RPC已经过时了，不需要重试
    if args.PrevLogIndex < rf.NextIndex[server]-1 {
        return
    }
    //向AE chan 申请重发
    select {
        case rf.AppendEntriesChan[server] <- serialNo + 1: 
        default:
        }
    return

}
```

3. AE成功但是reply.term比自己大，降为follower

```go
//返回的term比当前的term大，说明这个follower的term比leader大，需要降级
if reply.Term > rf.CurrentTerm {
    rf.toFollower(reply.Term)
    rf.ElectionAlarm =time.Now().Add(time.Duration(rand.Int63n(150)+150) * time.Millisecond)
    return
}
```

4. 如果返回成功,那么就更新NextIndex和MatchIndex以及CommitIndex，并在server的NextIndex变化时触发自己的rf.SnapshotTrigger

```go
if reply.Success {

    //如果成功，那么就更新该server的NextIndex和MatchIndex
    //arg.Entries在RPC中可能被截断或改变，成为实际被append的entries，所以下面直接加上
    oldNextIndex := rf.NextIndex[server]
    rf.NextIndex[server] = labutil.Max(args.PrevLogIndex+len(args.Entries)+1, rf.NextIndex[server])
    rf.MatchIndex[server] = labutil.Max(args.PrevLogIndex+len(args.Entries), rf.MatchIndex[server])
    lablog.Debug(rf.me, dTopicOfAppendEntriesRPC(args, lablog.Log), "%s RPC -> S%d success, updated NI:%v, MI:%v", rpcIntent, server, rf.NextIndex, rf.MatchIndex)

    go rf.updateCommitIndex(term)

    //* 在server的NextIndex被更新时，rf.SnapshotTrigger会被触发
    if oldNextIndex < rf.NextIndex[server] {
        select {
            case rf.SnapshotTrigger <- true:
        default:
            }
    }
    //及时ruturn
    return
}
```

5. 根据reply返回的冲突信息，重新制定AE或选择发送快照

```go
needToInstallSnapshot := false

if reply.XLen != 0 && reply.ConflictTerm == 0 {
    //reply.XLen= lastLogIndex + 1
    //即follower的最后一个Index比leader发送的PrevLogIndex短,从follower的log的最后一条开始传
    //但是可能reply.XLen对应的日志leader已经没有了，所以后续需要判断是否安装快照
    rf.NextIndex[server] = reply.XLen
} else {
    var entryIndex, entryTerm int
    for i := len(rf.Log) - 1; i >= -1; i-- {
        if i < 0 {
            entryIndex, entryTerm = rf.lastLogIndexAndTerm()
        } else {
            entryIndex, entryTerm = rf.Log[i].Index, rf.Log[i].Term
        }

        if entryTerm == reply.ConflictTerm {
            // leader中存在follower的ConflictTerm对应的日志，则从该term的最新日志开始传
            rf.NextIndex[server] = entryIndex + 1
            break
        }
        if entryTerm < reply.ConflictTerm {

            // leader中不存在follower的ConflictTerm对应的日志，则从上一个term的最后一个日志开始传
            //conflictIndex保存的是follower的log中term不等于conflictTerm的日志的最新的index
            rf.NextIndex[server] = reply.ConflictIndex
            break
        }
        //leader中的日志无法满足上述两种情况，那么就发送快照
        if i < 0 {

            needToInstallSnapshot = true
            rf.NextIndex[server] = rf.LastIncludedIndex + 1
            break
        }

    }
}
if needToInstallSnapshot || rf.NextIndex[server] <= rf.LastIncludedIndex {
    select {
        case rf.InstallSnapshotChan[server] <- 0:
    default:
        }
} else {
    select {
        case rf.AppendEntriesChan[server] <- 0: 
    default:
        }
}
```

###### ** Raft.AppendEntries()  RPC实际调用的函数

ticker()->collectVote()->entriesAppender()->appendEntries()->sendAppendEntries()->Raft.AppendEntries()

1. Term大小不同的处理情况

```go
reply.ConflictIndex = -1
reply.ConflictTerm = -1
reply.Success = false
reply.Term = rf.CurrentTerm

rf.mu.Lock()
defer rf.mu.Unlock()

//如果收到的term比自己的term小，那么就拒绝
if args.Term < rf.CurrentTerm || args.LeaderId == rf.me || rf.killed() {
    return
}
//如果收到的term比自己的term大，那么就转换成follower
if args.Term > rf.CurrentTerm {
    rf.toFollower(args.Term)
}

// 可能自己本身就是 Candidate
if rf.State == CANDIDATE && args.Term >= rf.CurrentTerm {
    rf.toFollower(args.Term)
}
//收到AE后，重置选举超时时间
rf.ElectionAlarm = time.Now().Add(time.Duration(rand.Intn(150)+150) * time.Millisecond)

```

2. 判断发送过来的AE中的日志与follower本身保存的日志是否有冲突情况

   1）lastLogIndex < args.PrevLogIndex：拒绝(follower太短），记录，返回

   2）根据args.PrevLogIndex 与rf.LastIncludedIndex确定follower在PrevLogIndex处的preLogTerm，判断prevLogTerm 与args.PrevLogTerm关系，不相等时，记录并返回

```go
lastLogIndex, _ := rf.lastLogIndexAndTerm()
//如果收到的AE中的prevLogIndex比自己的log的最后一个index还大，那么就拒绝，并且返回冲突的index
if lastLogIndex < args.PrevLogIndex {
    reply.XLen = lastLogIndex + 1
    // reply.ConflictIndex = lastLogIndex + 1
    return
}

var prevLogTerm int
switch {
    case args.PrevLogIndex == rf.LastIncludedIndex:
    prevLogTerm = rf.LastIncludedTerm
    case args.PrevLogIndex < rf.LastIncludedIndex:
    //leader发送的日志是从PrevLogIndex到最后一条日志，所以args log后面还有可以追加的部分
    args.PrevLogIndex = rf.LastIncludedIndex
    prevLogTerm = rf.LastIncludedTerm

    // 将args.Entries中的日志，裁剪到从LastIncludedIndex+1开始
    sameEntryInArgsEntries := false
    for i := range args.Entries {
        if args.Entries[i].Index == rf.LastIncludedIndex && args.Entries[i].Term == rf.LastIncludedTerm {
            sameEntryInArgsEntries = true
            args.Entries = args.Entries[i+1:]
            break
        }
    }
    if !sameEntryInArgsEntries {
        // 如果args.Entries中没有和LastIncludedIndex相同的日志，那么就清空args.Entries
        args.Entries = make([]LogEntry, 0)
    }
    default:
    // args.PrevLogIndex > rf.LastIncludedIndex
    prevLogTerm = rf.Log[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
}

//如果相同的index的term不同，那么就拒绝，并且返回冲突的term
if prevLogTerm != args.PrevLogTerm {

    //记录冲突时 follower的log中的term
    reply.ConflictTerm = prevLogTerm

    for i := args.PrevLogIndex - 1 - rf.LastIncludedIndex; i >= 0; i-- {
        //confilctIndex记录prevLogTerm之前term的最后一个Index,如果leader没有ConflictTerm时，则从这开始重新发送日志
        reply.ConflictIndex = rf.Log[i].Index
        if rf.Log[i].Term != prevLogTerm {

            break
        }
    }
    return
}
```

3. 接受成功，当传入的log不为空（可能在上一步中被裁剪），则可以追加

```go
reply.Success = true
//如果收到的AE中的entries不为空，那么就更新自己的log
if len(args.Entries) > 0 {
    //从prevLogIndex开始，向后查找，找到第一个不同的entry
    log_from_prev := rf.Log[args.PrevLogIndex:]
    var i int
    needsave := false
    for i = 0; i < len(args.Entries) && i < len(log_from_prev); i++ {
        if args.Entries[i].Term != log_from_prev[i].Term {
            //如果不同，那么就删除自己的log中从prevLogIndex开始的所有entry
            rf.Log = rf.Log[:args.PrevLogIndex+i]
            needsave = true
            //如果收到的AE中的entries不为空，那么就更新自己的log
            if len(args.Entries) > 0 {
                //从prevLogIndex+1对应的logEntry开始，向后查找，找到第一个不同的entry
                lablog.Debug(rf.me, lablog.Info, "Received: %v from S%d at T%d", args.Entries, args.LeaderId, args.Term)
                log_from_prev := rf.Log[args.PrevLogIndex-rf.LastIncludedIndex:]
                var i int
                needsave := false
                for i = 0; i < len(args.Entries) && i < len(log_from_prev); i++ {
                    if args.Entries[i].Term != log_from_prev[i].Term {
                        //如果不同，那么就删除自己的log中从prevLogIndex开始的所有entry
                        rf.Log = rf.Log[:args.PrevLogIndex-rf.LastIncludedIndex+i]
                        needsave = true
                        break
                    }
                }
                //两种情况，一种是从prevLogIndex开始，自己的log和收到的log完全一样，那么就直接追加收到的log
                //一种情况是从prevLogIndex开始，存在不同的项，但是已经在上面删除了，那么就直接追加收到的log
                if i < len(args.Entries) {
                    lablog.Debug(rf.me, lablog.Info, "Append new: %v from i: %d", args.Entries[i:], i)
                    rf.Log = append(rf.Log, args.Entries[i:]...)
                    needsave = true
                }
                //如果日志有更新，那么就持久化
                if needsave {
                    rf.persist()
                }

            }    break
        }
    }

    //两种情况，一种是从prevLogIndex开始，自己的log和收到的log完全一样，那么就直接追加收到的log
    //一种情况是从prevLogIndex开始，存在不同的项，但是已经在上面删除了，那么就直接追加收到的log
    if i < len(args.Entries) {
        rf.Log = append(rf.Log, args.Entries[i:]...)
        needsave = true
    }
    //如果日志有更新，那么就持久化
    if needsave {
        rf.persist()
    }

}
```

4. 判断自己的CommitIndex和LeaderCommit的大小，更新时通过CommitTrigger chan来更新状态机

```go
if args.LeaderCommit > rf.CommitIndex {
    rf.CommitIndex = labutil.Min(args.LeaderCommit, len(rf.Log)-1)
    select {
        case rf.CommitTrigger <- true:
    default:
        }
}
```

##### ·  ticker()->collectVote()->snapshotInstaller()    为每个server维持一个goroutine根据条件不断发送IS直到不再是leader

```go
func (rf *Raft) snapshotInstaller(server int, ch <-chan int, term int) {
    var lastArgs *InstallSnapshotArgs

    i := 1 //serialNo
    //该变量用于记录当前发送的snapshot的次数
    currentSnapshotnum := 0
    for !rf.killed() {

        //等待新的Installsnapshot RPC
        serialNo, ok := <-ch
        if !ok {
            return //如果channel已经关闭，那么就退出
        }
        rf.mu.Lock()

        //如果term发生变化，或者不是Leader 那么就不用发送了
        if rf.State != LEADER || rf.CurrentTerm != term || rf.killed() {
            rf.mu.Unlock()
            return
        }
        switch {
            //lastArgs里面的snapshot过时了，那么就重新构造
            //一开始的时候lastArgs是nil的，所以这里也会进入
            case lastArgs == nil || lastArgs.LastIncludedIndex < rf.LastIncludedIndex:
            lastArgs = rf.constructSnapshotArgs(server)
            currentSnapshotnum = 1
            i++
            //将serialNo与i比较，过时的就不用发送了
            case serialNo >= i:
            i++
            //如果发送的次数超过了3次，那么就不用发送了
            case rf.LastIncludedIndex == lastArgs.LastIncludedIndex && currentSnapshotnum < 3:
            currentSnapshotnum++
            default:
            rf.mu.Unlock()
            continue
        }

        go rf.installSnapshot(server, term, lastArgs, i)

        rf.mu.Unlock()
    }
}
```

###### ·  ticker()->collectVote()->snapshotInstaller() ->installSnapshot() 根据IS的RPC调用结果reply处理各种情况

1. 调用RPC函数发送snapshot

```go
reply := &InstallSnapshotReply{}
ok := rf.sendInstallSnapshot(server, lastArgs, reply)
```

2. 调用完RPC后判断是否还是leader，根据RPC是否成功，判断是否重发

```go
rf.mu.Lock()
defer rf.mu.Unlock()

if rf.State != LEADER || rf.killed() {
    return

}
if !ok {
    select {
        case rf.InstallSnapshotChan[server] <- serialNo + 1:
        default:
        }
    return
}
```

3. 根据reply的term判断是否自己需要toFollower

```go
if reply.Term > rf.CurrentTerm {
    lablog.Debug(rf.me, lablog.Term, "IS <- S%d Term is higher(%d > %d), following", server, reply.Term, rf.CurrentTerm)

    rf.toFollower(reply.Term)
    return
}

if rf.CurrentTerm != term {
    return
}
```

4. 发送成功后，更新NextIndex[server]以及MatchIndex[server],并根据NextIndex[server]是否变大来判断是否需要保存之前的快照

```go
//与appendEntries不同,没有冲突的情况
//success
oldNextIndex := rf.NextIndex[server]
rf.NextIndex[server] = labutil.Max(lastArgs.LastIncludedIndex+1, rf.NextIndex[server])
rf.MatchIndex[server] = labutil.Max(lastArgs.LastIncludedIndex, rf.MatchIndex[server])
lablog.Debug(rf.me, lablog.Snap, "IS RPC -> S%d success, updated NI:%v, MI:%v", server, rf.NextIndex, rf.MatchIndex)
if rf.NextIndex[server] > oldNextIndex {
    select {
        case rf.SnapshotTrigger <- true:
    default:
        }
}
```

###### ** Raft.InstallSnapshot()（RPC函数)

ticker()->ticker()->collectVote()->snapshotInstaller() ->installSnapshot()->sendInstallSnapshot()->InstallSnapshot()（RPC函数)

1. 根据传来的term和自己的term，判断是否拒绝，是否toFollower

```go
rf.mu.Lock()
defer rf.mu.Unlock()
reply.Term = rf.CurrentTerm
if args.Term < rf.CurrentTerm || rf.killed() || args.LeaderId == rf.me {
    return
}
if args.Term > rf.CurrentTerm {
    rf.toFollower(args.Term)
}
rf.ElectionAlarm = nextElectionAlarm()
```

2. 判断传来的snapshot是否过时，否则更新LastIncludedIndex, LastIncluedTerm,CommitIndex,LastApplied

```go
if args.LastIncludedIndex <= rf.LastIncludedIndex ||
args.LastIncludedIndex <= rf.LastApplied {
    //传来的snapshot已经过时了
    return
}

rf.LastIncludedIndex = args.LastIncludedIndex
rf.LastIncludedTerm = args.LastIncludedTerm

rf.CommitIndex = labutil.Max(rf.CommitIndex, rf.LastIncludedIndex)

//上面已经判断LastIncludedIndex > LastApplied了
rf.LastApplied = rf.LastIncludedIndex
```

3. 删除快照之前的日志

```go
for i := range rf.Log {
    if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
        // if existing log entry has same index and term as snapshot's last included entry,
        // retain log entries following it and reply
        rf.Log = rf.Log[i+1:]
        return
    }
}
```

4. 将该快照直接化保存，并触发自己的CommitTrigger来读取该快照，应用到状态机上

```go
defer func() {

    rf.saveStateAndSnapshot(args.Data)
    //?
    if rf.CommitTrigger != nil {
        // going to send snapshot to service
        // CANNOT lose this trigger signal, MUST wait channel sending done,
        // so cannot use select-default scheme
        //go是如何实现等待channel发送完成的呢？
        go func(ch chan<- bool) { ch <- false }(rf.CommitTrigger)
        // upon received a snapshot, must notify upper-level service ASAP,
        // before ANY new commit signal,
        // so set commitTrigger to nil to block other goroutines from sending to this channel.
        // committer will re-enable this channel once it start to process snapshot and send back to upper-level service
        rf.CommitTrigger = nil
    }
}()
```

## commiter()

当发送过来的值是false时，则需要安装snapshot(leader发过来了snapshot)

```go
for !rf.killed() {
    isCommit, ok := <-triggerCh

    if !ok {
        return
    }
    rf.mu.Lock()
    //不是commit,当需要安装snapshot则会给CommitTrigger发送false，
    if !isCommit {
        // re-enable commitTrigger to be ready to accept commit signal
        在上一步安装snapshot函数中CommitTrigger被置为nil，这里需要恢复
        rf.CommitTrigger = triggerCh

        // is received snapshot from leader
        data := rf.persister.ReadSnapshot()
        if rf.LastIncludedIndex == 0 || len(data) == 0 {
            // snapshot data invalid
            rf.mu.Unlock()
            continue
        }

        // send snapshot back to upper-level service
        applyMsg := ApplyMsg{
            CommandValid:  false,
            SnapshotValid: true,
            Snapshot:      data,
            SnapshotIndex: rf.LastIncludedIndex,
            SnapshotTerm:  rf.LastIncludedTerm,
        }
        rf.mu.Unlock()

        applyCh <- applyMsg
        continue
    }
```

当值为true时,则是commit

```go
//如果是commit，那么就把log中的command发送到applyCh中
for rf.CommitTrigger != nil && rf.LastApplied < rf.CommitIndex {
    rf.LastApplied++
    logEntry := rf.Log[rf.LastApplied-1-rf.LastIncludedIndex]
    rf.mu.Unlock()
    applyCh <- ApplyMsg{
        CommandValid: true,
        Command:      logEntry.Command,
        CommandIndex: logEntry.Index,
    }

    rf.mu.Lock()
}
rf.mu.Unlock()
}
```

## snapshoter()

1. snapshoter被触发的两种channel

```go
var index int
var snapshot []byte

//go语言 channel本质上是一个指针，所以这里的SnapshotCh是一个指针
cmdCh := rf.SnapshotCh

for !rf.killed() {
    select {
        case cmd := <-cmdCh:
        index, snapshot = cmd.Index, cmd.Snapshot
        case _, ok := <-triggerCh://triggerCh就是rf.SnapshotTrigger
        if !ok {
            return
        }

    }
```

其中rf.SnapshotTrigger被触发时，存在如下情况：（不会有新的snapshot传过来，只是被触发，看看需不需要保存之前的旧快照）

1. toFollower()函数，变成follower时，触发自己的SnapshotTrigger，看看有无需要保存的快照
2. Leader的NextIndex[server]变大时，会触发自己的SnapshotTrigger（可能在向follower安装snapshot后，可能是发送AE后）

rf.SnapshotCh不知道怎么被触发

2. 判断是否保存之前的快照

```go
//如果是leader，那么需要考虑暂停保存快照，因为leader需要等待大多数的follower都保存了快照才能继续保存快照
shouldSuspend := rf.shouldSuspendSnapshot(index)

if cmdCh == nil {

    //对于leader来说需要暂停保存快照时，会在下面switch中将cmdCh置为nil
    if shouldSuspend {
        rf.mu.Unlock()
        continue
    }
    //准备好接受新的snapshot command
    cmdCh = rf.SnapshotCh
}

switch {
    case index <= rf.LastIncludedIndex: //目前snapshot的index比已经保存的还要小，那么就不用保存了,已经过时了
case shouldSuspend:
    cmdCh = nil
    default:
    //保存快照，更改LastIncludedIndex,LastIncludedTerm，Log
    rf.LastIncludedTerm = rf.Log[index-rf.LastIncludedIndex-1].Term
    rf.Log = rf.Log[index-rf.LastIncludedIndex-1+1:]
    rf.LastIncludedIndex = index

    rf.saveStateAndSnapshot(snapshot)

}
rf.mu.Unlock()
```

