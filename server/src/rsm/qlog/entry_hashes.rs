//! An entry record without the `Append` hash lists its message records hold
//! ([`super::record::REC_ENTRY_ELIDED`]).
//!
//! The log writer puts each entry's message records into a queue log and then,
//! in the same write and under the same fsync, the entry's payload-free record.
//! Both carried every `Append`'s hash list: 16 bytes a message, twice in one
//! file (`benchmark-queen/2026-10-15-bytes-per-msg`). Here the entry record
//! leaves out the lists of the `Append`s whose message record that log holds;
//! a reader puts them back from those records before anything decodes the
//! entry, so every caller sees the payload-free bytes exactly as they were.
//!
//! The elided form keeps the ORIGINAL entry frame header — `body_len | xxh3`
//! of the whole payload-free body — in front of the shortened body, so a
//! restore is checked against what was written: a hash list put back from the
//! wrong record is a checksum mismatch, never a different entry. The record's
//! own checksum covers the elided bytes on disk.
//!
//! Byte-level, on the wire form of `entry.rs` (`commands`, then `effects`,
//! each framed `kind | version | len | body`) and the payload-free `Append`
//! body of `effect.rs` (`pid | bucket | base | count | created | blob(hashes)
//! | blob(frame length)`): no decode, so an `Append` without its list is never
//! a value anything could validate or apply.

use std::io;

use xxhash_rust::xxh3::xxh3_64;

/// `Kind::Append` (permanent, like every effect kind id).
const APPEND: u16 = 7;
/// `body_len:u32 | xxh3:u64` in front of an entry body.
const FRAME: usize = 4 + 8;
/// `format | kinds_version | now | pid_base | kv_version_base`.
const ENTRY_FIXED: usize = 2 + 4 + 8 + 8 + 8;
/// `request_id | first_effect | effect_count | tag | version`, then the
/// outcome's `len:u32` and body.
const COMMAND_FIXED: usize = 16 + 4 + 4 + 2 + 2;
/// The payload-free `Append` body before its hash list's length:
/// `pid | bucket | base_offset | count | created_at`.
const APPEND_FIXED: usize = 8 + 2 + 8 + 4 + 8;

fn bad(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("entry record hash list: {what}"),
    )
}

fn u16_at(b: &[u8], at: usize) -> io::Result<u16> {
    b.get(at..at + 2)
        .map(|s| u16::from_le_bytes(s.try_into().expect("2 bytes")))
        .ok_or_else(|| bad("short"))
}

fn u32_at(b: &[u8], at: usize) -> io::Result<u32> {
    b.get(at..at + 4)
        .map(|s| u32::from_le_bytes(s.try_into().expect("4 bytes")))
        .ok_or_else(|| bad("short"))
}

fn u64_at(b: &[u8], at: usize) -> io::Result<u64> {
    b.get(at..at + 8)
        .map(|s| u64::from_le_bytes(s.try_into().expect("8 bytes")))
        .ok_or_else(|| bad("short"))
}

/// Copy `body` (an entry body) into `out`, rewriting each `Append` effect's
/// body with `edit(pid, base_offset, count, hashes) -> the hash list to
/// write`; every other byte as it is.
fn rewrite(
    body: &[u8],
    out: &mut Vec<u8>,
    mut edit: impl FnMut(u64, u64, u32, &[u8]) -> io::Result<Option<Vec<u8>>>,
) -> io::Result<bool> {
    let mut changed = false;
    let mut at = ENTRY_FIXED;
    let ncmd = u32_at(body, at)? as usize;
    at += 4;
    for _ in 0..ncmd {
        let olen = u32_at(body, at + COMMAND_FIXED)? as usize;
        at += COMMAND_FIXED + 4 + olen;
    }
    if at + 4 > body.len() {
        return Err(bad("commands overrun the body"));
    }
    out.extend_from_slice(&body[..at + 4]);
    let neff = u32_at(body, at)? as usize;
    at += 4;
    for _ in 0..neff {
        let kind = u16_at(body, at)?;
        let blen = u32_at(body, at + 4)? as usize;
        let start = at + 8;
        let eff = body
            .get(start..start + blen)
            .ok_or_else(|| bad("an effect overruns the body"))?;
        if kind != APPEND {
            out.extend_from_slice(&body[at..start + blen]);
            at = start + blen;
            continue;
        }
        let pid = u64_at(eff, 0)?;
        let base = u64_at(eff, 8 + 2)?;
        let count = u32_at(eff, 8 + 2 + 8)?;
        let hlen = u32_at(eff, APPEND_FIXED)? as usize;
        let hashes = eff
            .get(APPEND_FIXED + 4..APPEND_FIXED + 4 + hlen)
            .ok_or_else(|| bad("a hash list overruns its effect"))?;
        let tail = &eff[APPEND_FIXED + 4 + hlen..];
        match edit(pid, base, count, hashes)? {
            None => out.extend_from_slice(&body[at..start + blen]),
            Some(new) => {
                changed = true;
                let new_len = APPEND_FIXED + 4 + new.len() + tail.len();
                out.extend_from_slice(&body[at..at + 4]); // kind | version
                out.extend_from_slice(&(new_len as u32).to_le_bytes());
                out.extend_from_slice(&eff[..APPEND_FIXED]);
                out.extend_from_slice(&(new.len() as u32).to_le_bytes());
                out.extend_from_slice(&new);
                out.extend_from_slice(tail);
            }
        }
        at = start + blen;
    }
    if at != body.len() {
        return Err(bad("trailing bytes after the effects"));
    }
    Ok(changed)
}

/// The elided form of `entry` (payload-free entry bytes: frame + body): every
/// `Append` for which `held(pid, base_offset)` says its message record is in
/// the same log loses its hash list. `None` when no list was left out (write
/// the record as it is).
pub fn elide(entry: &[u8], held: impl Fn(u64, u64) -> bool) -> io::Result<Option<Vec<u8>>> {
    let body = entry.get(FRAME..).ok_or_else(|| bad("short frame"))?;
    let mut out = Vec::with_capacity(entry.len());
    // The ORIGINAL frame header: what a restore must rebuild.
    out.extend_from_slice(&entry[..FRAME]);
    let changed = rewrite(body, &mut out, |pid, base, _count, hashes| {
        Ok((!hashes.is_empty() && held(pid, base)).then(Vec::new))
    })?;
    Ok(changed.then_some(out))
}

/// The payload-free entry bytes `elide` started from: every `Append` without a
/// hash list gets `hashes_of(pid, base_offset, count)` back (16 bytes a
/// message), and the result must be the body the kept frame header names.
pub fn restore(
    elided: &[u8],
    mut hashes_of: impl FnMut(u64, u64, u32) -> io::Result<Vec<u8>>,
) -> io::Result<Vec<u8>> {
    let header = elided.get(..FRAME).ok_or_else(|| bad("short frame"))?;
    let want_len = u32_at(header, 0)? as usize;
    let want_sum = u64_at(header, 4)?;
    let mut out = Vec::with_capacity(FRAME + want_len);
    out.extend_from_slice(header);
    rewrite(&elided[FRAME..], &mut out, |pid, base, count, hashes| {
        if !hashes.is_empty() || count == 0 {
            return Ok(None);
        }
        let h = hashes_of(pid, base, count)?;
        if h.len() != 16 * count as usize {
            return Err(bad("a message record's hash list has another length"));
        }
        Ok(Some(h))
    })?;
    let body = &out[FRAME..];
    if body.len() != want_len || xxh3_64(body) != want_sum {
        return Err(bad("the restored entry is not the one written"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsm::effect::Effect;
    use crate::rsm::entry::{encode_entry_payload_free, Entry, Outcome, PushOutcome, PushVerdict};

    fn entry() -> (Entry, Vec<u8>, Vec<u8>) {
        let h1: Vec<u8> = (0..3 * 16).map(|i| i as u8).collect();
        let h2: Vec<u8> = (0..2 * 16).map(|i| 200u8.wrapping_add(i as u8)).collect();
        let mut e = Entry::new(1_768_000_000_000_000, 9, 1);
        let push = |pid, n: u64| {
            Outcome::Push(PushOutcome {
                items: (0..n)
                    .map(|k| PushVerdict::Created {
                        pid,
                        offset: k,
                        created_at_us: 5,
                    })
                    .collect(),
                compact: false,
            })
        };
        e.add_command(
            [1; 16],
            push(4, 3),
            vec![Effect::Append {
                pid: 4,
                bucket: 0,
                base_offset: 0,
                count: 3,
                created_at_us: 5,
                hashes: h1.clone(),
                blob: vec![7; 300],
            }],
        )
        .unwrap();
        e.add_command(
            [2; 16],
            push(6, 2),
            vec![
                Effect::Noop,
                Effect::Append {
                    pid: 6,
                    bucket: 0,
                    base_offset: 0,
                    count: 2,
                    created_at_us: 5,
                    hashes: h2.clone(),
                    blob: vec![8; 200],
                },
            ],
        )
        .unwrap();
        (e, h1, h2)
    }

    #[test]
    fn an_elided_entry_restores_to_the_bytes_written() {
        let (e, h1, h2) = entry();
        let pf = encode_entry_payload_free(&e).unwrap();
        // Both appends' records are in this log.
        let el = elide(&pf, |_, _| true)
            .unwrap()
            .expect("something left out");
        assert_eq!(el.len(), pf.len() - (3 + 2) * 16);
        let back = restore(&el, |pid, base, count| {
            assert_eq!(base, 0);
            Ok(match (pid, count) {
                (4, 3) => h1.clone(),
                (6, 2) => h2.clone(),
                other => panic!("{other:?}"),
            })
        })
        .unwrap();
        assert_eq!(back, pf, "byte for byte");
        // Only one of them in this log: the other keeps its list.
        let el = elide(&pf, |pid, _| pid == 6)
            .unwrap()
            .expect("one left out");
        assert_eq!(el.len(), pf.len() - 2 * 16);
        let back = restore(&el, |pid, _, _| {
            assert_eq!(pid, 6, "only the elided one is asked for");
            Ok(h2.clone())
        })
        .unwrap();
        assert_eq!(back, pf);
        // None in this log: nothing to write differently.
        assert_eq!(elide(&pf, |_, _| false).unwrap(), None);
    }

    #[test]
    fn a_restore_from_the_wrong_record_is_refused() {
        let (e, h1, _) = entry();
        let pf = encode_entry_payload_free(&e).unwrap();
        let el = elide(&pf, |pid, _| pid == 4).unwrap().unwrap();
        let mut wrong = h1.clone();
        wrong[0] ^= 1;
        assert!(
            restore(&el, |_, _, _| Ok(wrong.clone())).is_err(),
            "another list"
        );
        assert!(
            restore(&el, |_, _, _| Ok(h1[..16].to_vec())).is_err(),
            "another length"
        );
        assert!(
            restore(&el, |_, _, _| Err(io::Error::other("gone"))).is_err(),
            "no record"
        );
    }
}
