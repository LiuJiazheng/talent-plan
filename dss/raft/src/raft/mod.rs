//use std::sync::mpsc::{Sender, Receiver,channel};
use std::sync::{Arc,Mutex, atomic::{AtomicBool,Ordering,AtomicI32}};
use std::thread;
use std::time::{Duration, Instant};

use futures::sync::mpsc::{UnboundedSender, channel, Sender, Receiver};
use labrpc::RpcFuture;

use random_integer;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;


use futures_timer::Delay;
use futures::prelude::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub voted_for : u64,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
    /// Who is this node voting for
    /// protocal : since it's encode, using Optional is not ideal
    /// since encoder couldn't a naive way to store them
    /// So we use 0 present None,
    /// use x + 1 present x
    pub fn voted_for(&self) -> u64 {
        self.voted_for
    }

    pub fn new() -> Self {
        State{
            term : 0,
            is_leader : false,
            voted_for : 0,
        }
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        crate::your_code_here((rf, apply_ch))
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    /// query part
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx
        // ```
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        let peer = &self.peers[server];
        let peer_limit = self.peers.len();
        let (tx, rx) : (Sender<Result<RequestVoteReply>>,Receiver<Result<RequestVoteReply>>) = channel(peer_limit);
        peer.spawn(
            peer.request_vote(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res);
                    Ok(())
                }),
        );
        rx
    }

    fn send_apply_entry(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_limit = self.peers.len();
        let (tx, rx): (Sender<Result<AppendEntriesReply>>, Receiver<Result<AppendEntriesReply>>) = channel(peer_limit);
        peer.spawn(
            peer.append_entries(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res);
                    Ok(())
                }),
        );
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, &Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
    }
}


#[derive(Clone,Debug,PartialEq)]
enum NodeStatus{
    LEADER,
    FOLLOWER,
    CANDIDATE,
    SUICIDE,
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    raft: Arc<Mutex<Raft>>,                       // raft state machine
    node_status : Arc<Mutex<NodeStatus>>,         // a node status showing its role 
    poison : (Arc<AtomicBool>,Arc<AtomicI32>),    // a mechanism to help a follower kill itself
    tx : Sender<NodeStatus>,
    mail_box : Receiver<NodeStatus>,
}

// no-interrupt event loop
macro_rules! event_loop {
    ($duration : expr , $b : block) => {
        let before = Instant::now();
        loop{
            let esclapsed = Instant::now() - before;
            if esclapsed > $duration {break;}
            // payload
            $b;
        }
    };
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        crate::your_code_here(raft)
    }

    /// a dispatcher
    /// tick and following become_* function should return immediately
    #[inline]
    fn tick(&self,transmission : NodeStatus) {
        let post = self.tx.clone();
        post.try_send(transmission);
    }

    fn mail_daemon(&self) {
        loop{
            let msg = self.mail_box.poll();
            match msg {
                Ok(Async::NotReady) => {},
                Err(_) => {break;},
                Ok(Async::Ready(status)) => {
                    match status {
                        None => {panic!("Should Not Happen!");},
                        Some(NodeStatus::CANDIDATE) => {thread::spawn(|| {self.become_candidate()});},
                        Some(NodeStatus::LEADER) => {thread::spawn( || {self.become_leader()});},
                        Some(NodeStatus::FOLLOWER) => {
                            // try to use another channel, use an atomic as msg
                            self.poison.0.store(true, Ordering::Relaxed);
                            // spin itself
                            // why there is no condition var? because here we are in MPSC
                            // the SCP model makres sure here we handle the event sequently
                            // and if we block the thread, then no further thread can move on
                            loop{
                                let (poisoned,semaphore) = self.poison;
                                // if another thread has suicide itself, or the semaphore is positive,
                                // which means it is still available
                                if !poisoned.load(Ordering::Relaxed) || semaphore.load(Ordering::Relaxed) > 0 {break;}
                            }
                            self.poison.0.store(false, Ordering::Relaxed);
                            self.poison.1.store(0, Ordering::Relaxed);
                            thread::spawn(|| {self.become_follower()});
                        },
                        Some(NodeStatus::SUICIDE) => {break;},
                    }
                }
            }
        }
    }

    #[inline]
    fn random_duration_generator(lower : usize, upper : usize) -> Duration {
        let rand = random_integer::random_u64(lower as u64, upper as u64);
        Duration::from_millis(rand)
    }

    // blocking there
    fn become_follower(&self){
        // need to update the status of node!
        let new_state = Arc::new(State{term:self.term(),is_leader:false,voted_for:self.voted_for()});
        let mut raft_ptr = self.raft.lock().unwrap();
        (*raft_ptr).state = new_state;
        *(self.node_status.lock().unwrap()) = NodeStatus::FOLLOWER;
        // event loop for checking the its clock,
        // issue a election when timeout
        let duration = Self::random_duration_generator(100, 450);
        event_loop!(duration,{
            // when the node status has change, there is no meaning to continue any work
            // in this thread, so just exit in advance
            if *(self.node_status.lock().unwrap()) != NodeStatus::FOLLOWER {return;}
            // a new follower status is issued, so it needs to suicide itself and make thread have chance to suicide
            let (poisoned, semaphore) = self.poison;
            if poisoned.load(Ordering::Relaxed) {
                self.poison.0.store(false,Ordering::Relaxed);
                self.poison.1.store(1,Ordering::Relaxed);
                return;
            }
        });
        //issue a new candidate status
        self.tick(NodeStatus::CANDIDATE); 
    } 

    fn become_candidate(&self)  {
        let mut raft_ptr = self.raft.lock().unwrap();
        // need to update the status of node!
        let myself_id = (*raft_ptr).me;
        // buffer a term
        let myself_term = self.term() + 1;
        let new_state = Arc::new(State{term: myself_term, is_leader:false, voted_for:myself_id as u64});
        // update the state
        (*raft_ptr).state = new_state;
        // get the peer number, will be used later
        let peer_number = (*raft_ptr).peers.len();
        //  change interal status
        *(self.node_status.lock().unwrap()) = NodeStatus::CANDIDATE;
        // async issue vote request
        let mut receivers_pool : Vec<Receiver<Result<RequestVoteReply>>> = 
                             Vec::with_capacity(peer_number);
        for index in 0..peer_number {
            if index != myself_id {
                let server = index as usize;
                let request_arg = RequestVoteArgs{
                    term : self.term(),
                    candidate_id : self.voted_for(),
                };
                receivers_pool.push((*raft_ptr).send_request_vote(server,&request_arg));
            }
        }
        // a counter to count how many votes itself gets
        let mut counter = 1;  // first we have the vote from ourselves.
        // a flag used for exiting
        let mut exit_flag = false;
        // generate a leader election timeout
        let dur = Self::random_duration_generator(100, 450);
        // election process
        // net IO blocking !
        event_loop!(dur, {
            // try to fetch future result from sender side
            for rx in &mut receivers_pool {
                // rx is the recevier of a future, poll is TRY TO fetch data
                // from stream, it is never blocked.
                if let Ok(Async::Ready(Some(Ok(vote_reply)))) = rx.poll(){
                    if vote_reply.term == myself_term && vote_reply.vote_granted == true {
                        counter += 1;
                    }
                    if vote_reply.term > myself_term { 
                        exit_flag = true; 
                        // convert itself to follower
                        self.tick(NodeStatus::FOLLOWER);
                        break; 
                    }
                }
            }
            if *(self.node_status.lock().unwrap()) != NodeStatus::CANDIDATE {exit_flag = true;}
            if counter >= (peer_number + 1)/2 {break;}  
            if exit_flag {break;}
        });
        // elegantly close the channel
        for rx in &mut receivers_pool {
            rx.close();
        }
        // interrupt in an early stage
        if exit_flag {return;}
        if counter >= (peer_number + 1) / 2 {
            self.tick(NodeStatus::LEADER);
        } else {
            // otherwise issue new candidate
            self.tick(NodeStatus::CANDIDATE);
        }
    }

    fn become_leader(&self) {
        let mut raft_ptr = self.raft.lock().unwrap();
        // need to update the status of node!
        let myself_id = (*raft_ptr).me;
        let myself_term = self.term();
        // change state to leader, leader won't vote
        let new_state = Arc::new(State{term:myself_term, is_leader:true, voted_for:0});
        // update the state
        (*raft_ptr).state = new_state;
        // get the peer number, will be used later
        let peer_number = (*raft_ptr).peers.len();
        //  change interal status
        *(self.node_status.lock().unwrap()) = NodeStatus::LEADER;
        
        let append_timeout = Self::random_duration_generator(50, 100);
        // run forver as server
        loop{
            let base_time = Instant::now();
            // async issue vote request
            let mut receivers_pool : Vec<Receiver<Result<AppendEntriesReply>>> = 
            Vec::with_capacity(peer_number);
            for index in 0..peer_number {
                if index != myself_id {
                    let server = index as usize;
                    // 2A : a dummy append instruction
                    let entries : [u8;20] = [0;20];
                    let request_arg = AppendEntriesArgs{
                    term           : self.term(),
                    leader_id      : myself_id as u64,
                    prev_log_index : 0,
                    prev_log_term  : 0,
                    leader_commit  : 0,
                    entries        : entries.to_vec(),
                    };
                    receivers_pool.push((*raft_ptr).send_apply_entry(server,&request_arg));
                }
            }
            // wait the reponse until timeout
            let mut count = 1;
            loop{
                for rx in &mut receivers_pool {
                    // rx is the recevier of a future, poll is TRY TO fetch data
                    // from stream, it is never blocked.
                    if let Ok(Async::Ready(Some(Ok(append_entries_reply)))) = rx.poll(){
                        if append_entries_reply.term > myself_term {
                            // exit !
                            self.tick(NodeStatus::FOLLOWER);
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Common part of all servers, no matter which state it is in
    /// It has strong side effect, it will change status of self
    fn common_react(&self) {
        let _=1;
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.raft.lock().unwrap().state.term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.raft.lock().unwrap().state.is_leader()
    }

    pub fn voted_for(&self) -> u64 {
        self.raft.lock().unwrap().state.voted_for()
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    // response part
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // 2A
        self.common_react();

        let vote_reply = if args.term < self.term(){
                            RequestVoteReply{term : self.term(),vote_granted : false}
                        }else{
                            RequestVoteReply{term : args.term,vote_granted : true}
                        };
        Box::new(futures::future::result(Ok(vote_reply)))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // Your code here (2A, 2B).
        // 2A

        self.common_react();  //side effect!

        let append_reply =  if args.term < self.term(){
                                AppendEntriesReply{term : self.term(), success : false}
                            } else{
                                AppendEntriesReply{term : args.term, success : true}
                            };
        Box::new(futures::future::result(Ok(append_reply)))
    }
}


/*
    to_do list:
    1. common react : when a higher term comes, convert itself
    2. for follower itself : even a normal vote/hearbeat rpc, it needs to issue a follower command
    3. tick shall be a send(msg) async function x
    4. needs a routine looping to issue real node status -> mailbox x
    5. finish leader, notice when it gets response, it can change the leader status x
    6. refine leader and candidate, mute them properly, not lead to a crash x
*/