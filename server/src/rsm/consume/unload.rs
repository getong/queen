//! Dropping the parts nobody needs in memory.
//!
//! The engine loads a whole-queue group's parts lazily — every partition of
//! the queue at the group's first wildcard contact, every partition created
//! since as it is created — and used to keep each one until its partition or
//! its group was deleted. A partition is deleted only once it has been empty
//! and idle for `PARTITION_CLEANUP_DAYS` (30 by default), so a workload that
//! keeps opening partitions (a chat per conversation, a key per entity) grew
//! the leader by every (group, partition) pair it ever touched: ~1.5 KB a pair
//! on 2.0.0, +130 MiB/h on the prod leader once smartchat moved (2026-10-05).
//! The 09-30 soak stayed flat only because its 1M partitions were all touched
//! in its first minutes.
//!
//! A part that holds nothing is a copy of its cursor row: no lease, nothing
//! to deliver (its cursor at the partition's tail), no hold, no timer, no
//! reservation, no dead letters, no answer waiting on it, every change durable.
//! Such a part, unchanged for `QUEEN_CONSUME_IDLE_UNLOAD_S`, is dropped, and its
//! partition keeps the group among its watchers. The next append there finds
//! the watcher without a part and puts the partition on the group's `late`
//! list — the path a partition created after the group's load already takes —
//! then wakes the group: its next pop, a parked one included, loads the part
//! back from the committed cursor row ([`Engine::ensure_part`]) as a new leader
//! would. Nothing of a dropped part lived only in memory, so nothing changes
//! but where it is read from.
//!
//! Only whole-queue groups ([`Load::Full`]) lose parts. A partial group's parts
//! are the partitions single-partition commands named, and no append would
//! bring one back.

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::rsm::effect::Pid;

use super::state::{lock, read, Gid, Group, GroupShard, Load, Part, PidInfo, Shard, SHARDS};
use super::{Engine, SEC_US};

/// The unloader takes a turn this often: one slice of one shard.
const PERIOD_US: i64 = 5_000;

/// Partitions one turn looks at. A turn holds its shard's lock, and whatever
/// needs a partition of that shard waits for it, the serve thread included
/// (the unloader runs in its clock work): so a turn must be short however
/// large the shard. The unloader used to take a whole shard in one `retain`,
/// once a second: with ten million partitions that is 625,000 parts and about
/// 80 ms under the lock, and 7% of all messages were delivered up to 80 ms
/// late whatever the load (three machines, 2026-10-12: end to end p99 74 ms at
/// 100,000 msg/s, against 11 ms with one million partitions). A slice of this
/// size is about 0.1 ms.
const SLICE: usize = 1024;

/// A pass over every shard begins no sooner than this after the one before
/// began: what the unloader did when it took a shard a second. A pass that
/// takes longer (ten million partitions: about 50 s) is followed at once by
/// the next.
const PASS_EVERY_US: i64 = SHARDS as i64 * SEC_US;

/// At most one log line about it this often.
const LOG_EVERY_US: i64 = 60 * SEC_US;

/// The unloader's pacing and counters.
#[derive(Default)]
pub(crate) struct Unload {
    /// The next turn (engine clock, µs).
    next_us: AtomicI64,
    /// Where the pass is. Two clocks tick the engine (the serve thread and the
    /// checkpoint ticker): the one that finds this taken skips its turn.
    scan: Mutex<Scan>,
    /// Parts dropped and parts loaded (first contacts and loads back), since
    /// this engine was built.
    pub unloaded: AtomicU64,
    pub loaded: AtomicU64,
    /// Claims whose frames had no row any more and were read from the queue
    /// log with no shard lock held ([`super::pop::Claimed::Cold`]).
    pub cold_claims: AtomicU64,
    /// Parts dropped since the last log line, and when that was.
    since_log: AtomicU64,
    logged_us: AtomicI64,
    /// Tests: the clock work takes no turn. The engine's suite drives the
    /// unloader by hand and counts what each pass drops; a turn of the clock
    /// work in between (every command of the harness ticks the engine) would
    /// drop some of it first.
    #[cfg(test)]
    pub auto_off: std::sync::atomic::AtomicBool,
}

/// The unloader's place in its pass over the shards.
#[derive(Default)]
struct Scan {
    /// The shard the pass is in.
    shard: usize,
    /// The next partition of that shard to look at; `None`: the shard begins.
    next: Option<Pid>,
    /// The groups that may lose parts, decided when the shard begins.
    whole: HashSet<Gid>,
    /// The groups that lost a part in this shard: their tables are given back
    /// when the shard is done.
    lost: HashSet<Gid>,
    /// When the pass began.
    pass_started_us: i64,
}

/// The engine's size: what the leader holds in memory for consumption.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub groups: usize,
    pub parts: usize,
    pub partitions: usize,
    pub unloaded_total: u64,
    pub loaded_total: u64,
    pub cold_claims_total: u64,
}

/// Whether nothing of the part lives only in memory or is due to happen in it:
/// its cursor row says everything a load back needs. (A timer still pending
/// is no reason to stay: without a lease and without a hold it is the expiry
/// of a lease already acked, and a timer finding no part does nothing.)
fn unloadable(p: &Part, pi: &PidInfo) -> bool {
    !p.queued
        && !p.dirty
        && !p.delete_row
        && p.cur.lease.is_none()
        && p.reserved.is_none()
        && p.ready_at == 0
        && p.seed_ts.is_none()
        && p.waiters.is_empty()
        && p.dlq.is_empty()
        && p.dlq_sent.is_empty()
        && p.durable_ver == p.ver
        && p.sent_ver <= p.durable_ver
        && p.doubt_ver <= p.durable_ver
        && pi.tail <= p.cur.committed
}

impl Engine {
    /// The clock work's turn: one slice of one shard.
    pub(crate) fn unload_idle(&self, now: i64) {
        if self.k.idle_unload_us <= 0 || now < self.unload.next_us.load(Ordering::Acquire) {
            return;
        }
        #[cfg(test)]
        if self.unload.auto_off.load(Ordering::Acquire) {
            return;
        }
        let Ok(mut scan) = self.unload.scan.try_lock() else {
            return;
        };
        self.unload
            .next_us
            .store(now + PERIOD_US, Ordering::Release);
        let (n, _) = self.unload_turn(&mut scan, now, SLICE);
        drop(scan);
        if n == 0 {
            return;
        }
        let since = self.unload.since_log.fetch_add(n, Ordering::AcqRel) + n;
        if now.saturating_sub(self.unload.logged_us.load(Ordering::Acquire)) >= LOG_EVERY_US {
            self.unload.logged_us.store(now, Ordering::Release);
            self.unload.since_log.store(0, Ordering::Release);
            let s = self.engine_stats();
            tracing::info!(target: "rsm", unloaded = since, parts = s.parts, partitions = s.partitions,
                groups = s.groups, idle_s = self.k.idle_unload_us / SEC_US,
                "consume: idle parts unloaded (loaded back on their next append)");
        }
    }

    /// One turn of the pass: at most `budget` partitions of the shard the pass
    /// is in, from where the last turn stopped. Returns how many parts went and
    /// whether the pass over every shard ended with this turn.
    fn unload_turn(&self, scan: &mut Scan, now: i64, budget: usize) -> (u64, bool) {
        if self.k.idle_unload_us <= 0 {
            return (0, true);
        }
        // A reset waits for this to end: no part of an incarnation that is
        // going is looked at after it went ([`Engine::serving`]).
        let _serving = read(&self.serving);
        if !self.leader.load(Ordering::Acquire) {
            *scan = Scan::default();
            return (0, true);
        }
        if scan.next.is_none() {
            if scan.shard == 0 {
                scan.pass_started_us = now;
            }
            scan.whole = self.whole_groups();
            scan.lost.clear();
        }
        let (dropped, next) = self.unload_range(
            scan.shard,
            scan.next,
            budget,
            now,
            false,
            &scan.whole,
            &mut scan.lost,
        );
        scan.next = next;
        let mut pass_done = false;
        if next.is_none() {
            scan.shard = (scan.shard + 1) % SHARDS;
            if scan.shard == 0 {
                pass_done = true;
                self.unload.next_us.store(
                    (scan.pass_started_us + PASS_EVERY_US).max(now + PERIOD_US),
                    Ordering::Release,
                );
            }
        }
        self.unload.unloaded.fetch_add(dropped, Ordering::AcqRel);
        (dropped, pass_done)
    }

    /// One turn with a budget of the caller's (tests: that a pass made of
    /// small turns does what one whole pass does).
    #[cfg(test)]
    pub(crate) fn unload_turn_for_test(&self, now: i64, budget: usize) -> (u64, bool) {
        let mut scan = lock(&self.unload.scan);
        self.unload_turn(&mut scan, now, budget)
    }

    /// Where the pass is: the shard, and the next partition there (tests).
    #[cfg(test)]
    pub(crate) fn unload_cursor_for_test(&self) -> (usize, Option<Pid>) {
        let scan = lock(&self.unload.scan);
        (scan.shard, scan.next)
    }

    /// Every shard at once, by the idle rule (tests).
    #[cfg(test)]
    pub(crate) fn unload_all(&self, now: i64) -> u64 {
        let all: Vec<usize> = (0..SHARDS).collect();
        self.unload_shards(&all, now, false)
    }

    /// Some shards, by the idle rule (tests: the cost of one pass).
    #[cfg(test)]
    pub(crate) fn unload_shards_for_test(&self, shards: &[usize], now: i64) -> u64 {
        self.unload_shards(shards, now, false)
    }

    /// Every part that could go, idle or not (tests: the suite run with
    /// `QUEEN_TEST_IDLE_UNLOAD_US` drops them before every command).
    #[cfg(test)]
    pub(crate) fn unload_now(&self, now: i64) -> u64 {
        let all: Vec<usize> = (0..SHARDS).collect();
        self.unload_shards(&all, now, true)
    }

    /// Drop the idle parts of whole-queue groups in `shards`, each shard in
    /// one go (`force`: every part that could go, however recently it
    /// changed): how many went.
    #[cfg(test)]
    fn unload_shards(&self, shards: &[usize], now: i64, force: bool) -> u64 {
        if self.k.idle_unload_us <= 0 {
            return 0;
        }
        let _serving = read(&self.serving);
        if !self.leader.load(Ordering::Acquire) {
            return 0;
        }
        let whole = self.whole_groups();
        let mut dropped = 0u64;
        for &si in shards {
            let mut lost = HashSet::new();
            dropped += self
                .unload_range(si, None, usize::MAX, now, force, &whole, &mut lost)
                .0;
        }
        self.unload.unloaded.fetch_add(dropped, Ordering::AcqRel);
        dropped
    }

    /// The groups that may lose parts, decided before any shard is taken
    /// (lock order: a group's `st`, then one shard). A group that stops being
    /// one meanwhile only lost parts it could load back.
    fn whole_groups(&self) -> HashSet<Gid> {
        let groups: Vec<Arc<Group>> = read(&self.reg).by_id.values().cloned().collect();
        let mut whole: HashSet<Gid> = HashSet::with_capacity(groups.len());
        for g in &groups {
            let st = lock(&g.st);
            if st.load == Load::Full && !st.dropped && !st.bulk_pending {
                whole.insert(g.id);
            }
        }
        whole
    }

    /// Look at the partitions `from`, `from + SHARDS`, ... of shard `si`
    /// (`None`: from its lowest), `budget` of them at most, and drop the parts
    /// of `whole` groups that are idle (`force`: that could go, however
    /// recently they changed). Returns how many went, and where the next turn
    /// resumes (`None`: the shard is done).
    ///
    /// The walk is over partition ids, not over a group's table: ids are
    /// handed out in sequence, a shard holds every sixteenth, and a walk by
    /// id resumes at an id, where a walk of a hash table would have to skip
    /// to its place again every turn. An id nobody holds costs one lookup.
    #[allow(clippy::too_many_arguments)]
    fn unload_range(
        &self,
        si: usize,
        from: Option<Pid>,
        budget: usize,
        now: i64,
        force: bool,
        whole: &HashSet<Gid>,
        lost: &mut HashSet<Gid>,
    ) -> (u64, Option<Pid>) {
        let idle = self.k.idle_unload_us;
        let mut guard = lock(&self.shards[si]);
        let Shard {
            pids,
            groups,
            pid_lo,
            pid_hi,
            ..
        } = &mut *guard;
        let mut gss: Vec<(Gid, &mut GroupShard)> = groups
            .iter_mut()
            .filter(|(gid, gs)| whole.contains(*gid) && !gs.g.dead.load(Ordering::Acquire))
            .map(|(gid, gs)| (*gid, gs))
            .collect();
        if gss.is_empty() || pids.is_empty() {
            return (0, None);
        }
        let n = SHARDS as u64;
        let start = from.unwrap_or(*pid_lo);
        // The first id at or after `start` that this shard holds.
        let mut pid = start + (si as u64 + n - start % n) % n;
        let hi = *pid_hi;
        let per_pid = gss.len().max(1);
        let mut left = budget;
        let mut dropped = 0u64;
        let mut resume = None;
        while pid <= hi {
            if left < per_pid && left < budget {
                resume = Some(pid);
                break;
            }
            left = left.saturating_sub(per_pid);
            for (gid, gs) in gss.iter_mut() {
                let stays = match gs.parts.get_mut(&pid) {
                    None => continue,
                    Some(p) => {
                        let pi = pids.get(&pid);
                        if force {
                            pi.is_none_or(|pi| !unloadable(p, pi))
                        } else {
                            keep(p, pi, now, idle)
                        }
                    }
                };
                if !stays {
                    gs.parts.remove(&pid);
                    lost.insert(*gid);
                    dropped += 1;
                }
            }
            match pid.checked_add(n) {
                Some(p) => pid = p,
                None => break,
            }
        }
        if resume.is_none() {
            // The shard is done. A table keeps its size after removals: the
            // ones that lost parts in this pass give it back, once they hold
            // under a quarter of it, and keep room for twice what they hold.
            // (Shrunk to fit at half, a table whose parts come and go — loaded
            // by an append, dropped a window later — was rebuilt twice a pass:
            // grown when the next appends came, shrunk when their parts went.)
            for (gid, gs) in gss.iter_mut() {
                let len = gs.parts.len();
                if lost.contains(&*gid) && gs.parts.capacity() > (len * 4).max(64) {
                    gs.parts.shrink_to(len * 2);
                }
            }
        }
        (dropped, resume)
    }

    /// What the leader holds for consumption now (the gauges).
    pub fn engine_stats(&self) -> EngineStats {
        let groups = read(&self.reg).by_id.len();
        let (mut parts, mut partitions) = (0, 0);
        for s in self.shards.iter() {
            let sh = lock(s);
            partitions += sh.pids.len();
            parts += sh.groups.values().map(|gs| gs.parts.len()).sum::<usize>();
        }
        EngineStats {
            groups,
            parts,
            partitions,
            unloaded_total: self.unload.unloaded.load(Ordering::Acquire),
            loaded_total: self.unload.loaded.load(Ordering::Acquire),
            cold_claims_total: self.unload.cold_claims.load(Ordering::Acquire),
        }
    }

    /// Appends found watchers without a part (dropped while idle): each
    /// partition goes on its group's `late` list and the group is woken, so
    /// its next pop — a parked one first — loads the part back. Called with
    /// no shard held.
    pub(crate) fn reload_parts(&self, reload: Vec<(Arc<Group>, Pid)>) {
        let mut woken: Vec<Gid> = Vec::with_capacity(reload.len());
        for (g, pid) in reload {
            {
                let mut st = lock(&g.st);
                if st.dropped || st.load == Load::Partial {
                    continue;
                }
                if !st.late.contains(&pid) {
                    st.late.push(pid);
                }
            }
            g.bump();
            woken.push(g.id);
        }
        woken.sort_unstable();
        woken.dedup();
        for gid in woken {
            self.wake_group(gid);
        }
    }
}

/// The unloader's look at one part: whether it stays.
fn keep(p: &mut Part, pi: Option<&PidInfo>, now: i64, idle: i64) -> bool {
    // Changed since the last look: idle from now on.
    if p.ver != p.seen_ver {
        p.seen_ver = p.ver;
        p.idle_since_us = now;
        return true;
    }
    let Some(pi) = pi else {
        return true;
    };
    let quiet_since = p.idle_since_us.max(pi.last_append_us);
    now.saturating_sub(quiet_since) < idle || !unloadable(p, pi)
}
