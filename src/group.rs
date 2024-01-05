use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use crate::order::{Timestamp, ViewIdentifier, ViewStamp};

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
#[repr(transparent)]
pub struct ModuleIdentifier(u128);

impl From<u128> for ModuleIdentifier {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
#[repr(transparent)]
pub struct GroupIdentifier(u128);

impl From<u128> for GroupIdentifier {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

pub struct Event {
    timestamp: Timestamp,
    kind: EventKind,
}

pub enum EventKind {
    Aborted {
        aid: TransactionIdentifier
    },
    Committing {
        participants: HashSet<ModuleIdentifier>,
        aid: TransactionIdentifier
    },
    Done {
        aid: TransactionIdentifier
    }
}

pub enum Status {
    Active,
    ViewManager,
    Underling
}

pub enum Info<State> {
    Read,
    Write
}
pub struct LockInfo<State> {
    locker: u128,
    info: Info<State>
}

pub struct Object<State> {
    uid: u128,
    base: State,
    lockers: HashSet<LockInfo<State>>
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct Cohort<State> {
    status: Status,
    group_state: HashSet<Object<State>>,
    group_id: GroupIdentifier,
    module_id: ModuleIdentifier,
    communication_buffer: BinaryHeap<Reverse<Event>>,
    view_id: ViewIdentifier,
    view: View,
    history: Vec<ViewStamp>,
}

impl<State> Cohort<State> {
    pub fn new(group_id: GroupIdentifier, module_id: ModuleIdentifier) -> Self {
        Self {
            status: Status::Active,
            group_state: HashSet::new(),
            group_id,
            module_id,
            communication_buffer: BinaryHeap::new(),
            view_id: Default::default(),
            view: Default::default(),
            history: Vec::new()
        }
    }
}

pub struct Configuration<State> {
    cohorts: HashSet<Cohort<State>>,
}

pub struct Group<State> {
    id: GroupIdentifier,
    configuration: Configuration<State>,
}

pub struct View {
    id: ViewIdentifier,
    primary: ModuleIdentifier,
    backups: HashSet<ModuleIdentifier>
}

pub struct Call {
    group: GroupIdentifier,
    view_stamp: ViewStamp
}

pub struct PSet {
    calls: HashSet<Call>
}

pub struct TransactionIdentifier {
    identifier: u128,
    group: GroupIdentifier,
    view_id: ViewIdentifier
}

pub struct Transaction {
    identifier: TransactionIdentifier,
}

pub struct CallIdentifier(u128);

pub enum Procedure {
    Prepare(PSet),
    Abort,
    Commit
}

pub struct Message {
    view_id: ViewIdentifier,
    call_id: CallIdentifier,
    procedure: Procedure
}

pub struct Reply {
    p_set: PSet
}

pub struct Client {
}
