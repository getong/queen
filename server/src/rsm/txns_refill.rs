//! The boot's refill of the `txns` rows' hash lists from the queue logs.
//!
//! A checkpoint stores a `txns` row without its hash list
//! ([`crate::rsm::dedup::txns_without_hashes`]): every one of those hashes is
//! already in the message record its append wrote to the queue log, and the
//! queue log keeps that record as long as the row can exist. A record dies
//! only below its partition's `txns_start` ([`crate::rsm::qlog::QLog`]'s
//! reclaim), and rows start at `rows_start`, never below `txns_start`
//! ([`crate::rsm::store::rows::PartitionRow::rows_start`]). So the store loads
//! the rows without them, and this step reads them back, one committed-frames
//! read per partition, after the queue logs open and before the replay or any
//! reader runs.
//!
//! A row whose record is gone is one a committed watermark, or a partition
//! delete, removed after the checkpoint was taken: the file's reclaim judged
//! it from applied state the checkpoint did not hold yet. That entry is above
//! the durable index, so the replay removes the row again before this node
//! leads (I13) or serves a read. Such a row is left as it is and counted.

use std::collections::HashMap;
use std::io;
use std::time::Instant;

use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::{Store, TypedReads};

/// What one refill did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Refilled {
    /// Rows the store loaded without their hash lists.
    pub rows: usize,
    /// Rows that took them back from the queue log.
    pub refilled: usize,
    /// Rows whose record the queue log no longer holds (removed again by the
    /// replay, see the module header).
    pub missing: usize,
    /// Rows of a partition the store no longer has (a delete still sweeping).
    pub orphaned: usize,
}

fn io_err(e: crate::rsm::store::StoreError) -> io::Error {
    io::Error::other(format!("txns refill: {e}"))
}

/// Put back the hash lists of every `txns` row `store` holds without them,
/// reading them from `reader`'s queue logs. A boot step: nothing else may
/// write the store meanwhile.
pub fn refill<S: Store + ?Sized>(store: &S, reader: &QLogReader) -> io::Result<Refilled> {
    let t0 = Instant::now();
    let groups = store.txns_without_hashes().map_err(io_err)?;
    let mut out = Refilled::default();
    if groups.is_empty() {
        return Ok(out);
    }
    for (pid, rows) in groups {
        out.rows += rows.len();
        let Some(part) = store.read(|r| r.partition(pid)).map_err(io_err)? else {
            out.orphaned += rows.len();
            continue;
        };
        let queue_id = QLogReader::queue_id_of(&part.tenant, &part.queue);
        let from = rows.first().map_or(0, |r| r.0);
        let to = rows.last().map_or(0, |r| r.1.saturating_add(1));
        let frames = reader.committed_frames(queue_id, pid, from, to, true)?;
        let by_base: HashMap<u64, &crate::rsm::qlog::CommittedFrame> =
            frames.iter().map(|f| (f.base_offset, f)).collect();
        let mut fill: Vec<(u64, Vec<u8>)> = Vec::with_capacity(rows.len());
        for (base, end) in &rows {
            let count = end.saturating_add(1).saturating_sub(*base);
            match by_base.get(base) {
                Some(f)
                    if f.end == end.saturating_add(1) && f.hashes.len() as u64 == 16 * count =>
                {
                    fill.push((*base, f.hashes.clone()))
                }
                _ => out.missing += 1,
            }
        }
        out.refilled += store.refill_txns(pid, &fill).map_err(io_err)?;
    }
    tracing::info!(
        target: "rsm",
        rows = out.rows,
        refilled = out.refilled,
        missing = out.missing,
        orphaned = out.orphaned,
        ms = t0.elapsed().as_millis() as u64,
        "txns rows refilled from the queue logs",
    );
    if out.missing > 0 {
        tracing::warn!(
            target: "rsm",
            missing = out.missing,
            "txns rows whose record the queue log no longer holds: a watermark after the \
             checkpoint removed them, and the replay removes them again",
        );
    }
    Ok(out)
}
