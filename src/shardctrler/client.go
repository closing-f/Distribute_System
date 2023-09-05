package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/labutil"
)

type Clerk struct {
	servers []*labrpc.ClientEnd //多服务器群
	me      int64               // client id
	leader  int                 // leader id
	opId    int                 // operation id,单调递增
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.me = labutil.Nrand() //随机生成一个client id
	ck.leader = 0
	ck.opId = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:      num,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	// Your code here.
	serverId := ck.leader //先从leader开始
	//TODO 改成for(servers)
	for NotOk := 0; NotOk < 10; {
		reply := &QueryReply{} //TODO &在函数参数里的用法
		ok := ck.servers[serverId].Call("ShardCtrler.Query", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			if !ok {
				NotOk++
			} else {
				NotOk = 0
			}
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return reply.Config
	}
	return Config{Num: -1} //为每个客户端限制请求次数，超过10次，返回空

}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:  servers,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader
	// Your code here.
	for {
		reply := &JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:     gids,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader
	// Your code here.
	for {
		reply := &LeaveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.me,
		OpId:     ck.opId,
	}
	ck.opId++
	serverId := ck.leader // send to leader first
	for {
		reply := &MoveReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrShutdown {
			serverId = (serverId + 1) % len(ck.servers)
			continue
		}
		if reply.Err == ErrInitElection {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		ck.leader = serverId
		return
	}
}
