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
        aid: TransactionIdentifier,
    },
    Done {
        aid: TransactionIdentifier
    },
}

pub enum Status {
    Active,
    ViewManager,
    Underling,
}

pub enum Info<State> {
    Read,
    Write,
}

pub struct LockInfo<State> {
    locker: u128,
    info: Info<State>,
}

pub type State = usize;

pub struct Object {
    uid: u128,
    base: State,
    lockers: HashSet<LockInfo<State>>,
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Default)]
pub struct ViewHistory(Vec<ViewStamp>);

impl ViewHistory {
    pub fn iter(&self) -> impl Iterator<Item=&ViewStamp> {
        self.0.iter()
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
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

pub struct View {
    id: ViewIdentifier,
    primary: ModuleIdentifier,
    backups: HashSet<ModuleIdentifier>,
}

pub struct Call {
    group: GroupIdentifier,
    view_stamp: ViewStamp,
}

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

pub struct TransactionIdentifier {
    identifier: u128,
    group: GroupIdentifier,
    view_id: ViewIdentifier,
}

pub struct Transaction {
    identifier: TransactionIdentifier,
}

pub struct CallIdentifier(u128);

pub enum Procedure {
    Prepare(ParticipantSet),
    Abort,
    Commit,
}

pub struct Message {
    view_id: ViewIdentifier,
    call_id: CallIdentifier,
    procedure: Procedure,
}

pub struct Reply {
    p_set: ParticipantSet,
}

pub struct Client {}

pub trait CommunicationBuffer {
    fn add(&mut self, event: Event);

    fn force_to(new_vs: ViewStamp);
}

pub enum Role {
    Primary(Primary)
}

pub struct Primary(Cohort);
