use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use crate::order::{Timestamp, ViewIdentifier, ViewStamp};

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
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

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Event {
    timestamp: Timestamp,
    kind: EventKind,
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum EventKind {
    Aborted {
        aid: TransactionIdentifier
    },
    Committing {
        participants: Vec<ModuleIdentifier>,
        aid: TransactionIdentifier,
    },
    Done {
        aid: TransactionIdentifier
    },
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum Status {
    Active,
    ViewManager,
    Underling,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Info {
    Read,
    Write,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LockInfo {
    locker: u128,
    info: Info,
}

pub type State = usize;

#[derive(Clone, Debug, PartialEq)]
pub struct Object {
    uid: u128,
    base: State,
    lockers: HashSet<LockInfo>,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
pub struct ViewHistory(Vec<ViewStamp>);

impl ViewHistory {
    pub fn iter(&self) -> impl Iterator<Item=&ViewStamp> {
        self.0.iter()
    }
}

#[derive(Clone, Debug)]
pub struct Cohort {
    status: Status,
    group_state: HashSet<Object>,
    group_id: GroupIdentifier,
    module_id: ModuleIdentifier,
    communication_buffer: BinaryHeap<Reverse<Event>>,
    view_id: ViewIdentifier,
    view: View,
    history: ViewHistory,
}

impl Cohort {
    pub fn new(group_id: GroupIdentifier, module_id: ModuleIdentifier) -> Self {
        Self {
            status: Status::Active,
            group_state: HashSet::new(),
            group_id,
            module_id,
            communication_buffer: BinaryHeap::new(),
            view_id: Default::default(),
            view: Default::default(),
            history: ViewHistory::default(),
        }
    }
}

pub struct Configuration {
    cohorts: HashSet<Cohort>,
}

pub struct Group {
    id: GroupIdentifier,
    configuration: Configuration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct View {
    id: ViewIdentifier,
    primary: ModuleIdentifier,
    backups: HashSet<ModuleIdentifier>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Call {
    group: GroupIdentifier,
    view_stamp: ViewStamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantSet {
    calls: HashSet<Call>,
}

impl ParticipantSet {

    pub fn view_stamp_max(&self, group: &GroupIdentifier) -> Option<ViewStamp> {
        self.calls.iter()
            .filter(|p| &p.group == group)
            .map(|p| p.view_stamp)
            .max()
    }

    pub fn compatible(&self, group: &GroupIdentifier, history: &ViewHistory) -> bool {
        self.calls.iter()
            .filter(|p| &p.group == group)
            .all(|p| history.iter().all(|v| p.view_stamp.id() != v.id() || p.view_stamp.timestamp() <= v.timestamp()))
    }
}

#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct TransactionIdentifier {
    identifier: u128,
    group: GroupIdentifier,
    view_id: ViewIdentifier,
}

pub struct Transaction {
    identifier: TransactionIdentifier,
}

#[derive(Copy, Clone, Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct CallIdentifier(u128);

impl From<u128> for CallIdentifier {
    fn from(value: u128) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Procedure {
    Prepare(ParticipantSet),
    Abort,
    Commit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    view_id: ViewIdentifier,
    call_id: CallIdentifier,
    procedure: Procedure,
}

impl Message {
    pub fn new(view: ViewIdentifier, call: CallIdentifier, procedure: Procedure) -> Self {
        Self {
            view_id: view,
            call_id: call,
            procedure
        }
    }
}

pub struct Reply {
    p_set: ParticipantSet,
}

pub struct Client {}

pub enum Role {
    Primary(Primary),
    Backup(Backup),
}

pub struct Primary(Cohort);
pub struct Backup(Cohort);
