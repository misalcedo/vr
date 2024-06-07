--------------------------------- MODULE VR ---------------------------------

(*

Viewstamped Replication Revisited TLA+ Specifiation

*)

EXTENDS Naturals, Sequences, FiniteSets, FiniteSetsExt, TLC

\* Used to limit the model size
CONSTANTS ClientCount,      \* the number of clients
          ReplicaCount,     \* the number of replicas in the cluster
          Operation,        \* the set of possible values a client can send as the operation in a request
          Result,           \* the set of possible values a client can receive as a result of the operation in a request
          N,                \* the larget natural number an interger value may be
          Nil               \* represents the absence of a value   

\* Validate the declared constants          
ASSUME /\ N \in Nat
       /\ N > 3
       /\ ReplicaCount \in 3..N \* The lowest supported cluster size is 3

\* Status
CONSTANTS Normal

Status == { Normal }

\* Protocol message types
CONSTANTS Request, Prepare, PrepareOk, Commit

\* Numerical type definitions
ViewNumber == 0..N
OpNumber == 0..N
CommitNumber == 0..N
Clients == 0..ClientCount
RequestNumber == 0..N
Replicas == 0..(ReplicaCount-1)

RequestMessage == [type: Request, op: Operation, c: Clients, s: RequestNumber]
PrepareMessage == [type: Prepare, v: ViewNumber, m: Request, n: OpNumber, k: CommitNumber]
PrepareOkMessage == [type: PrepareOk, v: ViewNumber, n: OpNumber, i: Replicas]
CommitMessage == [type: Commit, v: ViewNumber, k: CommitNumber]

\* The set of all possible protocol messages
Messages == RequestMessage
                \cup
            PrepareMessage 
                \cup
            PrepareOkMessage
                \cup
            CommitMessage

Envelope == [message: Messages, source: Replicas, destination: Replicas]
Reply == {Nil} \cup Result
CachedRequest == [request: RequestNumber, reply: Reply]
ClientTable == [Clients -> CachedRequest]
Prepared == [Replicas -> OpNumber]
Replica == [index: Replicas, status: Status, view: ViewNumber, last_normal_view: ViewNumber, op_number: OpNumber, committed: CommitNumber, log: Seq(RequestMessage), client_table: ClientTable, prepared: Prepared]
Client == [id: Clients, view: ViewNumber, last_request: RequestNumber, in_progress: BOOLEAN]

VARIABLES ClientState,          \* Tracks the state of each client
          ReplicaState,         \* Tracks the state of each replica
          Envelopes,            \* Tracks the pending deliveries of each messsage envelope
          DropProtocolMessages, \* Tracks whether a given replica drops incoming messages or not
          DropRequests          \* Tracks whether requests from a given client are dropped   

IsNil(a) ==
    a = Nil

NewEnvelope(message, source, destination) ==
    [message: message, source: source, destination: destination]

\* Send the message whether it already exists or not.
SendFunc(envelope, envelopes, pending_deliveries) ==
    IF envelope \in DOMAIN envelopes
    THEN [envelopes EXCEPT ![envelope] = @ + pending_deliveries]
    ELSE envelopes @@ (envelope :> pending_deliveries)

\* Remove a message from the bag of messages.
\* Used when a server is done processing a message.
DiscardFunc(envelope, envelopes) ==
    IF envelope \in DOMAIN envelopes
    THEN [envelopes EXCEPT ![envelope] = Max({0, @ - 1})]
    ELSE envelopes @@ (envelope :> 0)
    
\* Send a message to all replicas except self
BroadcastFunc(message, source, envelopes, replicas) ==
    LET broadcast == { NewEnvelope(message, source, d) : d \in replicas \ {source} }
        new == broadcast \ DOMAIN envelopes
    IN [envelope \in DOMAIN envelopes |-> 
            IF envelope \in broadcast
            THEN envelopes[envelope] + 1
            ELSE envelopes[envelope]] @@ [envelope \in new |-> 1]

Send(envelope) ==
    Envelopes' = SendFunc(envelope, Envelopes, 1)

SendAsReceived(envelope) ==
    Envelopes' = SendFunc(envelope, Envelopes, 0)

SendOnce(envelope) ==
    /\ envelope \notin DOMAIN Envelopes 
    /\ Envelopes' = SendFunc(envelope, Envelopes, 1)

Discard(envelope) ==
    Envelopes' = DiscardFunc(envelope, Envelopes)

Broadcast(message, source) ==
    Envelopes' = BroadcastFunc(message, source, Envelopes, Replicas)
    
DiscardAndBroadcast(envelope, message, source) ==
    /\ envelope \in DOMAIN Envelopes
    /\ Envelopes[envelope] > 0 \* message must have pending deliveries
    /\ Envelopes' = BroadcastFunc(message, source, DiscardFunc(envelope, Envelopes), Replicas)
    
DiscardAndSend(discard, send) ==
    /\ discard \in DOMAIN Envelopes
    /\ Envelopes[discard] > 0 \* message must have pending deliveries
    /\ Envelopes' = SendFunc(send, DiscardFunc(discard, Envelopes), 1)

ReceivableEnvelope(envelope, type, replica) ==
    /\ envelope.message.type = type
    /\ envelope.destination = replica
    /\ Envelopes[envelope] > 0 \* message must have pending deliveries

\* TODO: model sending replies to the client 
ReplyToClient(view, request, client) ==
    /\ request = ClientState[client].request
    /\ ClientState' = [ ClientState EXCEPT ![client] = [ @ EXCEPT !.view = view, !.in_progress = FALSE ] ]

GetStatus(replica) ==
    ReplicaState[replica].view
    
GetView(replica) ==
    ReplicaState[replica].view

GetLastNormalView(replica) ==
    ReplicaState[replica].last_normal_view
    
GetPrepared(replica, peer) ==
    ReplicaState[replica].prepared[peer]

GetLastRequest(replica, client) ==
    IF client \in DOMAIN ReplicaState[replica].client_table
    THEN ReplicaState[replica].client_table[client]
    ELSE Nil

Primary(view) ==
    view % ReplicaCount

IsPrimary(replica) ==
    Primary(GetView(replica)) = replica

GetOpNumber(replica) ==
    ReplicaState[replica].op_number

GetCommitted(replica) ==
    ReplicaState[replica].committed
    
GetLogEntry(replica, op_number) ==
    ReplicaState[replica].log[op_number - 1]
    
CanProgress(replica) ==
    DropProtocolMessages[replica] = FALSE

IsCommitted(replica, op_number) ==
    Quantify(Replicas, 
             LAMBDA peer : ReplicaState[replica].prepared[peer] >= op_number)
             >= ReplicaCount \div 2

CanRequest(client) ==
    DropRequests[client] = FALSE
    
RequestInProgress(client) ==
    ClientState[client].in_progress = TRUE
    
ClientView(client) ==
    ClientState[client].view
    
GetRequest(client) ==
    ClientState[client].request

NewCachedRequest(request) ==
    [request: request, reply: Nil]
    
NewRequestMessage(operation, client) ==
    [type: Request, op: operation, c: client, s: GetRequest(client)]

NewPrepareMessage(op_number, replica) ==
    [type: Prepare, v: GetView(replica), m: GetLogEntry(replica, op_number), n: op_number, k: GetCommitted(replica)]

NewPrepareOkMessage(op_number, replica) ==
    [type: PrepareOk, v: GetView(replica), n: op_number, i: replica]

NewCommitMessage(replica) ==
    [type: Commit, v: GetView(replica), k: GetCommitted(replica)]

NewReplica(replica) ==
    [index: replica, status: Normal, view: 0, last_normal_view: 0, op_number: 0, committed: 0, log: <<>>, prepared: [peer \in Replicas \ replica |-> 0]]
    
NewClient(client) ==
    [id: client, view: 0, request: 0, in_progress: FALSE]

TypeOk ==
    /\ \A client \in DOMAIN ClientState : client \in Client
    /\ \A replica \in DOMAIN ReplicaState : replica \in Replica /\ Cardinality(ReplicaState[replica].prepared) = (Replicas - 1)
    /\ \A envelope \in DOMAIN Envelopes : envelope \in Envelope /\ Envelopes[envelope] >= 0
    /\ \A replica \in DOMAIN DropProtocolMessages : DropProtocolMessages[replica] \in BOOLEAN
    /\ \A client \in DOMAIN DropRequests : DropRequests[client] \in BOOLEAN

Init ==
    /\ ClientState = [client \in Client |-> NewClient(client)]
    /\ ReplicaState = [replica \in Replicas |-> NewReplica(replica)]
    /\ Envelopes = <<>>
    /\ DropProtocolMessages = [replica \in Replicas |-> FALSE]
    /\ DropRequests = [client \in Clients |-> FALSE]

\* TODO: model client recovery behavior
SendRequest ==
    \E client \in Clients, operation \in Operation :
        /\ CanRequest(client)
        /\ ~RequestInProgress(client)
        /\ ClientState' = [ ClientState EXCEPT ![client] = [ @ EXCEPT !.request = @ + 1, !.in_progress = TRUE ] ]
        /\ LET primary == Primary(ClientView(client)) \* Clients are not replicas so primaries ignore the source on client requests.
           IN Send(NewEnvelope(NewRequestMessage(operation, client), primary, primary))
           
BroadcastRequest ==
    \E client \in Clients, operation \in Operation :
        /\ CanRequest(client)
        /\ RequestInProgress(client)
        /\ LET primary == Primary(ClientView(client)) \* Clients are not replicas so primaries ignore the source on client requests.
           IN Broadcast(NewRequestMessage(operation, client), primary)

ReceiveRequest ==
    \E replica \in Replicas, envelope \in DOMAIN Envelopes : 
        /\ CanProgress(replica)
        /\ ReceivableEnvelope(envelope, Request, replica)
        /\ GetStatus(replica) = Normal
        /\ IsPrimary(replica)
        /\ LET last_request == GetLastRequest(replica, envelope.message.c)
           IN IF IsNil(last_request) \/ (last_request.request < envelope.message.s /\ ~IsNil(last_request.reply))
              THEN /\ ReplicaState' = [ ReplicaState EXCEPT ![replica] = [ @ EXCEPT !.log = Append(@, envelope.message.m), !.op_number = @ + 1, !.client_state = @ @@ (envelope.message.c :> NewCachedRequest(envelope.message.s)) ] ]
                   /\ DiscardAndBroadcast(envelope, NewPrepareMessage(envelope.message.n, replica), replica)
              ELSE IF last_request.request = envelope.message.s /\ ~IsNil(last_request.reply)
                   THEN /\ Discard(envelope)
                        /\ ReplyToClient(GetView(replica), envelope.message.v, envelope.message.c)
                   ELSE Discard(envelope)

ReceivePrepare ==
    \E replica \in Replicas, envelope \in DOMAIN Envelopes : 
        /\ CanProgress(replica)
        /\ ReceivableEnvelope(envelope, Prepare, replica)
        /\ GetStatus(replica) = Normal
        /\ ~IsPrimary(replica)
        /\ envelope.message.v = GetView(replica)
        /\ envelope.message.n = GetOpNumber(replica) + 1
        /\ ReplicaState' = [ ReplicaState EXCEPT ![replica] = [ @ EXCEPT !.log = Append(@, envelope.message.m), !.op_number = @ + 1, !.committed = envelope.messsage.k ] ]
        /\ DiscardAndSend(envelope, NewEnvelope(NewPrepareOkMessage(envelope.message.n, replica), replica, envelope.source))

ReceivePrepareOk ==
    \E replica \in Replicas, envelope \in DOMAIN Envelopes : 
        /\ CanProgress(replica)
        /\ ReceivableEnvelope(envelope, PrepareOk, replica)
        /\ GetStatus(replica) = Normal
        /\ IsPrimary(replica)
        /\ envelope.message.v = GetView(replica)
        /\ envelope.message.n = GetPrepared(replica, envelope.source)
        /\ ReplicaState' = [ ReplicaState EXCEPT ![replica] = [ @ EXCEPT !.prepared = [ @ EXCEPT ![envelope.source] = envelope.message.n] ] ]
        /\ Discard(envelope)

ReceiveCommit ==
    \E replica \in Replicas, envelope \in DOMAIN Envelopes : 
        /\ CanProgress(replica)
        /\ ReceivableEnvelope(envelope, Prepare, replica)
        /\ GetStatus(replica) = Normal
        /\ ~IsPrimary(replica)
        /\ envelope.message.v = GetView(replica)
        /\ ReplicaState' = [ ReplicaState EXCEPT ![replica] = [ @ EXCEPT !.committed = envelope.messsage.k ] ]
        /\ Discard(envelope)

ExecuteUpCall ==
    \E replica \in Replicas, result \in Result :
        /\ CanProgress(replica)
        /\ IsPrimary(replica)
        /\ GetStatus(replica) = Normal
        /\ GetCommitted(replica) < GetOpNumber(replica)
        /\ IsCommitted(replica, GetCommitted(replica) + 1)
        /\ LET entry == GetLogEntry(replica, GetCommitted(replica) + 1)
           IN ReplicaState' = [ ReplicaState EXCEPT ![replica] = [ @ EXCEPT !.committed = @ + 1, !.client_state = @ @@ (entry.c :> [ NewCachedRequest(entry.s) EXCEPT !.reply = result ] ) ] ]

\* TODO: add unchanged to actions
Next ==
    \/ SendRequest
    \/ BroadcastRequest
    \/ ReceiveRequest
    \/ ReceivePrepare
    \/ ReceivePrepareOk
    \/ ReceiveCommit
    \/ ExecuteUpCall

=============================================================================
\* Modification History
\* Last modified Fri Jun 07 14:47:13 EDT 2024 by misalcedo
\* Created Fri Jun 07 08:30:43 EDT 2024 by misalcedo
