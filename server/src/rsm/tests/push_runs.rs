//! Catalogue version 7: a push outcome written as runs
//! ([`crate::rsm::entry::PushOutcome::compact`]).
//!
//! Version 1 writes one 25-byte verdict per message; a 100-message push
//! carried 2.5 KB of outcome in its entry and in its `request_ids` row, and
//! that row took a whole LMDB page at every checkpoint
//! (`benchmark-queen/2026-10-15-bytes-per-msg`). Version 7 writes the same
//! verdicts as runs, where the cluster reads it and where it is smaller.

use super::planner_harness::{effects, push, Cell};
use super::samples::*;
use crate::rsm::effect::*;
use crate::rsm::entry::*;
use crate::rsm::planner::txn::TxnOutcome;

fn created(pid: Pid, first: u64, n: u64, at: i64) -> Vec<PushVerdict> {
    (0..n)
        .map(|k| PushVerdict::Created {
            pid,
            offset: first + k,
            created_at_us: at,
        })
        .collect()
}

/// A version-7 push outcome, framed as a `request_ids` row holds it:
/// `tag | version | len | items | runs | run…`, built by hand.
fn hand_built(items: u32, runs: &[Vec<u8>]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&items.to_le_bytes());
    body.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for r in runs {
        body.extend_from_slice(r);
    }
    let mut out = Vec::new();
    out.extend_from_slice(&1u16.to_le_bytes()); // PUSH
    out.extend_from_slice(&VERSION_7.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn created_run(pid: u64, first: u64, at: i64, count: u32) -> Vec<u8> {
    let mut r = vec![0u8];
    r.extend_from_slice(&pid.to_le_bytes());
    r.extend_from_slice(&first.to_le_bytes());
    r.extend_from_slice(&at.to_le_bytes());
    r.extend_from_slice(&count.to_le_bytes());
    r
}

#[test]
fn a_hundred_message_push_is_one_run_of_29_bytes() {
    let o = PushOutcome::planned(created(3, 1_000, 100, 77), true);
    assert!(
        o.compact,
        "100 created messages of one partition are one run"
    );
    let runs = Outcome::Push(o.clone()).encode();
    // header 8 | items 4 | runs 4 | one created run 29
    assert_eq!(runs.len(), 8 + 4 + 4 + 29);
    let v1 = Outcome::Push(PushOutcome {
        items: o.items.clone(),
        compact: false,
    })
    .encode();
    assert_eq!(v1.len(), 8 + 4 + 100 * 25, "version 1: 25 bytes a message");
    assert_eq!(Outcome::decode(&runs).expect("decode"), Outcome::Push(o));
}

#[test]
fn runs_decode_to_the_same_verdicts_and_re_encode_byte_for_byte() {
    let o = compact_push_sample();
    let bytes = Outcome::Push(o.clone()).encode();
    assert_eq!(
        u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        5,
        "eight verdicts, five runs"
    );
    let back = Outcome::decode(&bytes).expect("decode");
    assert_eq!(
        back,
        Outcome::Push(o.clone()),
        "the flag is part of the value"
    );
    assert_eq!(
        back.encode(),
        bytes,
        "a decoded outcome re-encodes to its bytes"
    );

    // The same verdicts at version 1 decode to the same items.
    let v1 = Outcome::Push(PushOutcome {
        items: o.items.clone(),
        compact: false,
    });
    match Outcome::decode(&v1.encode()).expect("decode v1") {
        Outcome::Push(p) => {
            assert_eq!(p.items, o.items);
            assert!(!p.compact);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn runs_are_written_only_where_allowed_and_smaller() {
    // One message: 37 bytes as a run against 29 as a verdict.
    let one = PushOutcome::planned(created(3, 5, 1, 77), true);
    assert!(!one.compact);
    assert_eq!(Outcome::Push(one).version(), VERSION_1);
    // Two consecutive messages: 37 against 54.
    let two = PushOutcome::planned(created(3, 5, 2, 77), true);
    assert!(two.compact);
    assert_eq!(Outcome::Push(two).version(), VERSION_7);
    // Below version 7: never, whatever the size.
    assert!(!PushOutcome::planned(created(3, 5, 100, 77), false).compact);
    // Messages that share no run (every offset apart) stay at version 1.
    let scattered: Vec<PushVerdict> = (0..10)
        .map(|k| PushVerdict::Duplicate {
            pid: 3,
            offset: k * 2,
        })
        .collect();
    assert!(!PushOutcome::planned(scattered, true).compact);
}

#[test]
fn a_run_body_that_does_not_add_up_is_refused() {
    // The well-formed control.
    let ok = hand_built(3, &[created_run(1, 10, 5, 3)]);
    assert!(Outcome::decode(&ok).is_ok());

    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "runs short of the item count",
            hand_built(4, &[created_run(1, 10, 5, 3)]),
        ),
        (
            "runs past the item count",
            hand_built(2, &[created_run(1, 10, 5, 3)]),
        ),
        ("a run of zero", hand_built(0, &[created_run(1, 10, 5, 0)])),
        (
            "an offset past u64::MAX",
            hand_built(2, &[created_run(1, u64::MAX, 5, 2)]),
        ),
        ("an unknown run kind", {
            let mut r = created_run(1, 10, 5, 1);
            r[0] = 9;
            hand_built(1, &[r])
        }),
        (
            "an item count no body could carry",
            hand_built(u32::MAX, &[created_run(1, 0, 5, u32::MAX)]),
        ),
        ("a truncated run", {
            let mut r = created_run(1, 10, 5, 1);
            r.truncate(20);
            hand_built(1, &[r])
        }),
    ];
    for (what, bytes) in cases {
        assert!(Outcome::decode(&bytes).is_err(), "{what}: decoded");
    }
}

#[test]
fn an_entry_carrying_runs_is_a_version_7_entry() {
    let mut e = Entry::new(1, 2, 3);
    e.add_command(
        uuid(1),
        Outcome::Push(compact_push_sample()),
        vec![Effect::Noop],
    )
    .unwrap();
    assert_eq!(
        e.kinds_version, VERSION_7 as u32,
        "the outcome reaches the header"
    );
    let bytes = encode_entry(&e).expect("encode");
    assert_eq!(decode_entry(&bytes).expect("decode"), e);
    // The payload-free form, what the queue logs hold, carries it the same way.
    let free = encode_entry_payload_free(&e).expect("encode payload-free");
    assert_eq!(decode_entry(&free).expect("decode payload-free"), e);
}

#[test]
fn a_transaction_carrying_runs_is_minted_at_7() {
    let with_runs = TxnOutcome {
        pushes: vec![compact_push_sample()],
        ..TxnOutcome::default()
    };
    let o = with_runs.clone().into_outcome().expect("txn outcome");
    assert_eq!(o.version(), VERSION_7);
    let back = Outcome::decode(&o.encode()).expect("decode");
    assert_eq!(TxnOutcome::from_outcome(&back), Some(with_runs));

    let plain = TxnOutcome {
        pushes: vec![PushOutcome {
            items: created(3, 0, 2, 9),
            compact: false,
        }],
        ..TxnOutcome::default()
    };
    let o = plain.clone().into_outcome().expect("txn outcome");
    assert_eq!(o.version(), VERSION_1, "no runs, no new version");
    assert_eq!(TxnOutcome::from_outcome(&o), Some(plain));
}

#[test]
fn the_planner_writes_runs_once_the_cluster_reads_version_7() {
    let mut c = Cell::new("push-runs");
    let compact = |cy: &super::planner_harness::Cycle| match cy.outcome(0) {
        Outcome::Push(p) => p,
        other => panic!("not a push outcome: {other:?}"),
    };

    // A fresh store is at the baseline cluster version: one verdict an item.
    let cy = c.run(&[push(1, "q", "p", &["a", "b", "c"])]);
    assert!(
        !compact(&cy).compact,
        "below version 7 the outcome stays at 1"
    );

    // The cluster reaches 7: a push of three is one run.
    c.run(&[effects(2, vec![Effect::ClusterVersionSet { version: 7 }])]);
    let cy = c.run(&[push(3, "q", "p", &["d", "e", "f"])]);
    let p = compact(&cy);
    assert!(p.compact);
    let offsets: Vec<u64> = p
        .items
        .iter()
        .map(|v| match v {
            PushVerdict::Created { offset, .. } => *offset,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(offsets, vec![3, 4, 5]);

    // One message: version 1 is smaller, and stays.
    let cy = c.run(&[push(4, "q", "p", &["g"])]);
    assert!(!compact(&cy).compact);
}
