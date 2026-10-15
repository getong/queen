//! The checkpoint keeps the `txns` rows without their hash lists, the boot
//! puts them back from the queue logs ([`crate::rsm::txns_refill`]).
//!
//! A 100-message row was 1.6 KB in the checkpoint and LMDB gave every such
//! row a 4 KiB leaf page of its own; the hashes were in the message record
//! already (`benchmark-queen/2026-10-15-bytes-per-msg`).

use std::time::Duration;

use super::store::TempStore;
use crate::rsm::dedup::{self, TxnsRow};
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{PushReq, ReqCtx, Rsm, RsmBuildCtx};
use crate::rsm::store::keys;
use crate::rsm::store::rows::PartitionRow;
use crate::rsm::store::{HeedStore, Keyspace, Reads, Store, StoreOpts, TypedWrites, Writes};

fn row(end: u64, created: i64, n: usize, seed: u8) -> TxnsRow {
    TxnsRow {
        end,
        created_at_us: created,
        hashes: (0..n * 16).map(|i| seed.wrapping_add(i as u8)).collect(),
    }
}

fn opts(strip: bool) -> StoreOpts {
    StoreOpts {
        map_bytes: Some(64 << 20),
        txns_without_hashes: Some(strip),
        ..Default::default()
    }
}

#[test]
fn a_txns_row_leaves_its_hashes_out_and_takes_them_back_byte_for_byte() {
    let r = row(104, 7, 5, 3);
    for v in [r.encode(), r.encode_with_len(4321)] {
        let s = dedup::txns_without_hashes(&v).expect("hashes to leave out");
        assert!(dedup::txns_lacks_hashes(&s));
        assert!(!dedup::txns_lacks_hashes(&v));
        assert_eq!(s.len(), v.len() - 5 * 16);
        assert_eq!(
            dedup::txns_with_hashes(&s, &r.hashes),
            v,
            "the row apply wrote"
        );
        // What the store reads of it meanwhile: end, stamp and length kept.
        let d = TxnsRow::decode(&s).expect("decode");
        assert_eq!((d.end, d.created_at_us, d.count()), (104, 7, 0));
        assert_eq!(TxnsRow::retained_len_of(&s), TxnsRow::retained_len_of(&v));
        assert_eq!(
            dedup::txns_without_hashes(&s),
            None,
            "nothing left to leave out"
        );
    }
}

/// Two rows of partition 3 written whole into RAM and checkpointed.
fn write_rows(t: &TempStore) -> Vec<(u64, TxnsRow)> {
    let rows = vec![(100, row(104, 9, 5, 1)), (105, row(106, 10, 2, 99))];
    let mut w = t.s().write().expect("write");
    for (base, r) in &rows {
        w.put_raw(
            Keyspace::Txns,
            &keys::txns(3, *base),
            &r.encode_with_len(77),
        )
        .expect("put");
    }
    w.durable_commit().expect("durable commit");
    rows
}

#[test]
fn the_checkpoint_keeps_txns_rows_without_hashes_and_a_reopen_refills_them() {
    let mut t = TempStore::with(opts(true));
    let rows = write_rows(&t);
    let whole = |r: &TxnsRow| r.encode_with_len(77);

    // The image holds them without their hashes; RAM still has them whole.
    for (base, r) in &rows {
        let k = keys::txns(3, *base);
        let img = t
            .s()
            .checkpoint_get(Keyspace::Txns, &k)
            .expect("image read")
            .expect("in the image");
        assert!(
            dedup::txns_lacks_hashes(&img),
            "the checkpoint left them out"
        );
        assert_eq!(dedup::txns_with_hashes(&img, &r.hashes), whole(r));
        let ram = t
            .s()
            .read(|rd| Ok(rd.get_raw(Keyspace::Txns, &k)?.map(|v| v.to_vec())))
            .expect("read")
            .expect("in RAM");
        assert_eq!(ram, whole(r), "RAM keeps them whole");
    }
    assert!(t.s().txns_without_hashes().expect("list").is_empty());

    // A reopen loads them as the image holds them, and lists them.
    t.reopen(opts(true));
    let listed = t.s().txns_without_hashes().expect("list");
    assert_eq!(listed, vec![(3, vec![(100, 104), (105, 106)])]);

    // The refill puts them back, in RAM only and not dirty.
    let fill: Vec<(u64, Vec<u8>)> = rows.iter().map(|(b, r)| (*b, r.hashes.clone())).collect();
    assert_eq!(t.s().refill_txns(3, &fill).expect("refill"), 2);
    assert_eq!(
        t.s().dirty_len(Keyspace::Txns),
        0,
        "a refill is not a write"
    );
    for (base, r) in &rows {
        let k = keys::txns(3, *base);
        let ram = t
            .s()
            .read(|rd| Ok(rd.get_raw(Keyspace::Txns, &k)?.map(|v| v.to_vec())))
            .expect("read")
            .expect("in RAM");
        assert_eq!(ram, whole(r));
    }
    assert!(t.s().txns_without_hashes().expect("list").is_empty());
    // A second refill finds nothing to do; the seals verify.
    assert_eq!(t.s().refill_txns(3, &fill).expect("refill again"), 0);
    t.s().scrub().expect("scrub");
}

#[test]
fn a_store_that_keeps_them_whole_writes_them_whole() {
    let mut t = TempStore::with(opts(false));
    let rows = write_rows(&t);
    for (base, r) in &rows {
        let img = t
            .s()
            .checkpoint_get(Keyspace::Txns, &keys::txns(3, *base))
            .expect("image read")
            .expect("in the image");
        assert_eq!(img, r.encode_with_len(77));
    }
    t.reopen(opts(false));
    assert!(t.s().txns_without_hashes().expect("list").is_empty());
}

#[test]
fn a_row_whose_record_is_gone_is_counted_and_left_for_the_replay() {
    let mut t = TempStore::with(opts(true));
    {
        let mut w = t.s().write().expect("write");
        // Partition 3 exists; partition 9 is gone (a delete still sweeping).
        let p = PartitionRow::new([3; 16], "t", "q", "p", 1);
        w.put_partition(3, &p).expect("partition");
        w.put_raw(
            Keyspace::Txns,
            &keys::txns(3, 0),
            &row(1, 5, 2, 0).encode_with_len(1),
        )
        .expect("put");
        w.put_raw(
            Keyspace::Txns,
            &keys::txns(9, 0),
            &row(0, 5, 1, 0).encode_with_len(1),
        )
        .expect("put");
        w.durable_commit().expect("durable commit");
    }
    t.reopen(opts(true));
    // A queue log that holds none of their records.
    let qdir = t.dir.join("qlog-empty");
    let set =
        crate::rsm::qlog::set::QLogSet::new(qdir, crate::rsm::qlog::QLogOptions::testing(1 << 20));
    let got = crate::rsm::txns_refill::refill(t.s(), &set.reader()).expect("refill");
    assert_eq!(
        got,
        crate::rsm::txns_refill::Refilled {
            rows: 2,
            refilled: 0,
            missing: 1,
            orphaned: 1,
        }
    );
    assert_eq!(
        t.s().txns_without_hashes().expect("list").len(),
        2,
        "left as they were"
    );
}

fn ctx() -> ReqCtx {
    ReqCtx::new(
        crate::config::DEFAULT_TENANT,
        crate::rsm::facade::Deadline::after(Duration::from_secs(5)),
    )
}

async fn version_seven(facade: &RaftFacade) {
    for _ in 0..1_000 {
        if facade.cluster_version() >= u32::from(crate::rsm::effect::VERSION_7) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the cluster version stayed at {}", facade.cluster_version());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_restart_still_finds_the_duplicates_whose_hashes_the_checkpoint_left_out() {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-txns-refill-{}-{}",
        std::process::id(),
        rand::random::<u32>()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let build = RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    };
    let body = br#"{"items":[
        {"queue":"q","partition":"p","payload":{"v":1},"transactionId":"a"},
        {"queue":"q","partition":"p","payload":{"v":2},"transactionId":"b"},
        {"queue":"q","partition":"p","payload":{"v":3},"transactionId":"c"}
    ]}"#
    .to_vec();

    // Run 1: the cluster reaches 7, the push lands, a clean shutdown takes the
    // last checkpoint.
    {
        // The production form, without moving the process-wide dedup mode.
        crate::rsm::facade::real::TEST_TXNS_WITHOUT_HASHES.with(|c| c.set(Some(true)));
        let facade = RaftFacade::open(&build).expect("open 1");
        version_seven(&facade).await;
        let first = facade
            .push(ctx(), PushReq { raw: body.clone() })
            .await
            .expect("push");
        let v: serde_json::Value = serde_json::from_str(&first.body).expect("json");
        for (i, it) in v.as_array().expect("items").iter().enumerate() {
            assert_eq!(it["status"], "queued", "{}", first.body);
            assert_eq!(it["offset"].as_u64(), Some(i as u64));
        }
        facade.shutdown().await;
    }

    // The checkpoint holds the row without its hashes.
    {
        let store = HeedStore::open(&dir.join("store"), &StoreOpts::default()).expect("store");
        let listed = store.txns_without_hashes().expect("list");
        assert_eq!(
            listed.len(),
            1,
            "one partition's rows left their hashes out: {listed:?}"
        );
        assert_eq!(listed[0].1, vec![(0, 2)]);
        store.close();
    }

    // Run 2: the boot refilled them; the same transaction ids are duplicates
    // at their original offsets.
    {
        crate::rsm::facade::real::TEST_TXNS_WITHOUT_HASHES.with(|c| c.set(Some(true)));
        let facade = RaftFacade::open(&build).expect("open 2");
        let again = facade
            .push(ctx(), PushReq { raw: body })
            .await
            .expect("push again");
        let v: serde_json::Value = serde_json::from_str(&again.body).expect("json");
        for (i, it) in v.as_array().expect("items").iter().enumerate() {
            assert_eq!(
                it["status"], "duplicate",
                "after the restart: {}",
                again.body
            );
            assert_eq!(it["offset"].as_u64(), Some(i as u64));
        }
        facade.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}
