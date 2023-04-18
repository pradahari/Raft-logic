package raft

import "time"

// Handles an incoming RPC RequestVote request
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int

	Latency int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	CandidateId int

}

// RequestVote RPC. This is the function that is executed by the node that
// RECEIVES the RequestVote.
func (rn *RaftNode) HandleRequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	// Lock the RaftNode mutex to prevent concurrent access to shared data.
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Check if the RaftNode is dead.
	if rn.state == "Dead" {
		return nil
	}

	// Get the last log index and term.
	var lastLogIndex, lastLogTerm int
	if len(rn.log) > 0 {
		lastLogIndex = len(rn.log) - 1
		lastLogTerm = rn.log[lastLogIndex].Term
	} else {
		lastLogIndex = -1
		lastLogTerm = -1
	}

	// Log the vote request message if enabled.
	if LogVoteRequestMessages {
		rn.write_log("Received Vote Request from NODE %d; Args: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]",
			args.CandidateId, args, rn.currentTerm, rn.votedFor, lastLogIndex, lastLogTerm)
	}

	// Update current term if the received term is greater than current term.
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term)
	}

	// Check whether to grant the vote to the candidate or not.
	if (rn.votedFor == -1 || rn.votedFor == args.CandidateId) &&
		args.Term >= rn.currentTerm &&
		args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		// Grant vote to the candidate.
		rn.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		// Do not grant vote to the candidate.
		reply.VoteGranted = false
	}

	// Set the reply term to the current term.
	reply.Term = rn.currentTerm

	// Log the vote reply message if enabled.
	if LogVoteRequestMessages {
		rn.write_log("Sending Request Vote Reply: %+v", reply)
	}

	return nil
}

// Handles an incoming RPC AppendEntries request

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int

	Latency int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (this *RaftNode) HandleAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.state == "Dead" {
		return nil
	}

	var aeType string
	if len(args.Entries) > 0 {
		aeType = "AppendEntries"
	} else {
		aeType = "Heartbeat"
	}

	if (aeType == "Heartbeat" && LogHeartbeatMessages) || aeType == "AppendEntries" {
		this.write_log("Received %s from NODE %d; args: %+v", aeType, args.LeaderId, args)
	}

	if args.Term > this.currentTerm {
		this.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == this.currentTerm {
		if this.state != "Follower" {
			this.becomeFollower(args.Term)
		}
		this.lastElectionTimerStartedTime = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term matches PrevLogTerm?
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(this.log) && args.PrevLogTerm == this.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(this.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if this.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				this.log = append(this.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				this.write_log("Log is now: %v", this.log)
			}

			// Set commit index.
			if args.LeaderCommit > this.commitIndex {

				if args.LeaderCommit < len(this.log)-1 {
					this.commitIndex = len(this.log) - 1
				} else {
					this.commitIndex = args.LeaderCommit
				}

				this.notifyToApplyCommit <- 1
			}
		}
	}

	reply.Term = this.currentTerm
	if (aeType == "Heartbeat" && LogHeartbeatMessages) || aeType == "AppendEntries" {
		this.write_log("Sending %s reply: %+v", aeType, *reply)
	}
	return nil
}

// Either handle Command or tell to divert it to Leader
func (this *RaftNode) ReceiveClientCommand(command interface{}) bool {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.write_log("ReceiveClientCommand received by %s: %v", this.state, command)
	if this.state == "Leader" {
		this.log = append(this.log, LogEntry{Command: command, Term: this.currentTerm})
		this.write_log("Log=%v", this.log)
		return true
	}
	return false
}
