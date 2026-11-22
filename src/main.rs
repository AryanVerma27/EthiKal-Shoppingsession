//! Shopping Sessions pipeline: a single native pass implementing rules.md.
//!
//! Phases:
//!   A  parse every CSV chunk in parallel (events bucketed by event_id hash,
//!      valid clicks and link pairs kept per thread)
//!   L  union-find over links -> id -> person name
//!   B  per event bucket: dedup on event_id (min ingest_ts), keep valid copies,
//!      map to person, re-bucket by person hash; clicks mapped and bucketed too
//!   C  per person bucket: sort, sessionize, attribute, aggregate
//!   D  write Parquet

use std::fs;
use std::hash::{BuildHasher, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use hashbrown::HashMap;
use memchr::memchr;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const BBITS: u32 = 9;
const NB: usize = 1 << BBITS;
const CHUNK: usize = 8 << 20;

// ---------------------------------------------------------------- keys

/// IDs are 64-bit keys.
///
/// * Compact IDs (1..=10 bytes from `[0-9A-Za-z]`, which covers every ID the
///   generator writes) are an order-preserving base-63 number: byte-wise order
///   of the strings equals numeric order of the keys, and the top bit is 0.
/// * Any other ID is interned: top bit 1 plus an index. Equality stays exact;
///   ordering goes through `key_cmp`, which compares the real bytes.
type Key = u64;
const INTERNED: u64 = 1 << 63;

static RANK: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 37 + i as u8;
        i += 1;
    }
    t
};
static UNRANK: [u8; 63] = {
    let mut t = [0u8; 63];
    let mut i = 0;
    while i < 256 {
        if RANK[i] != 0 {
            t[RANK[i] as usize] = i as u8;
        }
        i += 1;
    }
    t
};
static POW63: [u64; 11] = {
    let mut t = [1u64; 11];
    let mut i = 1;
    while i < 11 {
        t[i] = t[i - 1] * 63;
        i += 1;
    }
    t
};

/// Lane masks keeping the first n bytes.
static KEEP: [[u8; 16]; 17] = {
    let mut t = [[0u8; 16]; 17];
    let mut n = 0;
    while n <= 16 {
        let mut j = 0;
        while j < n {
            t[n][j] = 0xFF;
            j += 1;
        }
        n += 1;
    }
    t
};

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn compact16(p: *const u8, l: usize) -> Option<Key> {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(p as *const __m128i);
    let in_range = |lo: u8, span: u8| {
        let t = _mm_sub_epi8(v, _mm_set1_epi8(lo as i8));
        _mm_cmpeq_epi8(_mm_min_epu8(t, _mm_set1_epi8(span as i8)), t)
    };
    let dg = in_range(b'0', 9);
    let up = in_range(b'A', 25);
    let lo = in_range(b'a', 25);
    let valid = _mm_or_si128(dg, _mm_or_si128(up, lo));
    let need = (1u32 << l) - 1;
    if (_mm_movemask_epi8(valid) as u32) & need != need {
        return None;
    }
    // rank = v - 47 (digit), v - 54 (upper), v - 60 (lower)
    let off = _mm_or_si128(
        _mm_and_si128(dg, _mm_set1_epi8(47)),
        _mm_or_si128(_mm_and_si128(up, _mm_set1_epi8(54)), _mm_and_si128(lo, _mm_set1_epi8(60))),
    );
    let r = _mm_and_si128(_mm_sub_epi8(v, off), _mm_loadu_si128(KEEP[l].as_ptr() as *const __m128i));
    // pairs: r0*63 + r1 ; quads: pair0*3969 + pair1
    let m1 = _mm_maddubs_epi16(r, _mm_setr_epi8(63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1, 63, 1));
    let m2 = _mm_madd_epi16(m1, _mm_setr_epi16(3969, 1, 3969, 1, 3969, 1, 3969, 1));
    let g0 = _mm_cvtsi128_si32(m2) as u32 as u64; // digits 0..4
    let g1 = _mm_extract_epi32(m2, 1) as u32 as u64; // digits 4..8
    let p4 = _mm_extract_epi16(m1, 4) as u32 as u64; // digits 8..10
    Some(g0 * POW63[6] + g1 * POW63[2] + p4)
}

/// Compact key, or None when the string needs interning.
#[inline(always)]
fn compact(s: &[u8]) -> Option<Key> {
    let l = s.len();
    if l > 10 {
        return None;
    }
    if l == 0 {
        // an empty slice may carry a dangling pointer: never load through it
        return Some(0);
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        let p = s.as_ptr();
        if (p as usize & 4095) <= 4096 - 16 {
            return unsafe { compact16(p, l) };
        }
    }
    let mut v = 0u64;
    let mut bad = 0u8;
    for &c in s {
        let r = RANK[c as usize];
        bad |= (r == 0) as u8;
        v = v * 63 + r as u64;
    }
    if bad != 0 {
        return None;
    }
    Some(v * POW63[10 - l])
}

struct Interned {
    map: std::collections::HashMap<Vec<u8>, u64>,
    strs: Vec<Vec<u8>>,
}
static INTERN: OnceLock<Mutex<Interned>> = OnceLock::new();
fn intern_table() -> &'static Mutex<Interned> {
    INTERN.get_or_init(|| Mutex::new(Interned { map: Default::default(), strs: Vec::new() }))
}

#[cold]
fn intern(s: &[u8]) -> Key {
    let mut t = intern_table().lock().unwrap();
    if let Some(&k) = t.map.get(s) {
        return k;
    }
    let k = INTERNED | t.strs.len() as u64;
    t.strs.push(s.to_vec());
    t.map.insert(s.to_vec(), k);
    k
}

#[inline(always)]
fn pack(s: &[u8]) -> Key {
    match compact(s) {
        Some(k) => k,
        None => intern(s),
    }
}

fn unpack(k: Key) -> Vec<u8> {
    if k & INTERNED != 0 {
        return intern_table().lock().unwrap().strs[(k & !INTERNED) as usize].clone();
    }
    let mut out = Vec::with_capacity(10);
    let mut v = k;
    let mut digits = [0u8; 10];
    for i in (0..10).rev() {
        digits[i] = (v % 63) as u8;
        v /= 63;
    }
    for d in digits {
        if d == 0 {
            break;
        }
        out.push(UNRANK[d as usize]);
    }
    out
}

/// Byte-wise order of the IDs behind two keys.
#[inline]
fn key_cmp(a: Key, b: Key) -> std::cmp::Ordering {
    if (a | b) & INTERNED == 0 {
        a.cmp(&b)
    } else {
        unpack(a).cmp(&unpack(b))
    }
}

#[inline(always)]
fn mix(k: u64) -> u64 {
    let mut x = k ^ 0x9E37_79B9_7F4A_7C15;
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[inline]
fn bucket_of(k: Key) -> usize {
    (mix(k) & (NB as u64 - 1)) as usize
}

#[derive(Default, Clone, Copy)]
struct KH;
struct KHasher(u64);
impl BuildHasher for KH {
    type Hasher = KHasher;
    fn build_hasher(&self) -> KHasher {
        KHasher(0)
    }
}
impl Hasher for KHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, b: &[u8]) {
        for &x in b {
            self.0 = (self.0.rotate_left(5) ^ x as u64).wrapping_mul(0x517C_C1B7_2722_0A95);
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = mix(v ^ 0x5bd1_e995_7f4a_7c15).rotate_left(23);
    }
}
type KMap<V> = HashMap<Key, V, KH>;

// ---------------------------------------------------------------- csv

#[inline]
fn unq(f: &[u8]) -> Option<&[u8]> {
    match memchr(b'"', f) {
        None => Some(f),
        Some(0) => {
            let n = f.len();
            if n >= 2 && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
                Some(&f[1..n - 1])
            } else {
                None
            }
        }
        Some(_) => None,
    }
}

#[inline]
fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut n = 0;
    let mut start = 0;
    for pos in memchr::memchr_iter(b',', line) {
        if n + 1 >= N {
            return None;
        }
        out[n] = unq(&line[start..pos])?;
        n += 1;
        start = pos + 1;
    }
    if n != N - 1 {
        return None;
    }
    out[n] = unq(&line[start..])?;
    Some(out)
}

#[inline(always)]
fn unq_strict(fld: &[u8]) -> Option<&[u8]> {
    let l = fld.len();
    if l < 2 || fld[0] != b'"' || fld[l - 1] != b'"' {
        return None;
    }
    let inner = &fld[1..l - 1];
    if inner.iter().any(|&c| c == b'"') {
        return None;
    }
    Some(inner)
}

/// (comma, quote, newline) bitmasks of the 64 bytes at `p`.
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let m = |c: u8| {
            let v = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
            lo | (hi << 32)
        };
        (m(b','), m(b'"'), m(b'\n'))
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let x = *p.add(i);
            c |= ((x == b',') as u64) << i;
            q |= ((x == b'"') as u64) << i;
            n |= ((x == b'\n') as u64) << i;
        }
        (c, q, n)
    }
}

/// Single pass over a chunk using 64-byte separator bitmasks: split lines on
/// '\n' and fields on ',', apply the quoting rule, and call `f` for every
/// well-formed line with exactly N fields.
///
/// Per field we record its end offset and its number of quote characters. A
/// field is valid iff it has no quotes, or exactly two that wrap it.
#[inline(always)]
fn scan<const N: usize>(data: &[u8], mut f: impl FnMut(&[&[u8]; N])) {
    let n = data.len();
    let mut ends = [0usize; 8];
    let mut qn = [0u32; 8];
    debug_assert!(N < 8);
    let mut nf = 0usize; // fields seen in the current line
    let mut ls = 0usize; // line start
    let mut qcarry = 0u32;
    let mut buf = [0u8; 64];
    let mut base = 0usize;

    #[inline(always)]
    fn emit<'a, const N: usize>(
        data: &'a [u8],
        ls: usize,
        ends: &[usize; 8],
        qn: &[u32; 8],
        f: &mut impl FnMut(&[&'a [u8]; N]),
    ) {
        let mut out: [&[u8]; N] = [&[]; N];
        let mut st = ls;
        for j in 0..N {
            let e = ends[j];
            // SAFETY: ls <= ends[0] < ends[1] < ... <= data.len() by construction
            let fld = unsafe { data.get_unchecked(st..e) };
            if qn[j] != 0 {
                let l = fld.len();
                if qn[j] != 2 || l < 2 || fld[0] != b'"' || fld[l - 1] != b'"' {
                    return;
                }
                out[j] = unsafe { fld.get_unchecked(1..l - 1) };
            } else {
                out[j] = fld;
            }
            st = e + 1;
        }
        f(&out);
    }

    while base < n {
        let (mc, mq, mn) = unsafe {
            if base + 64 <= n {
                masks64(data.as_ptr().add(base))
            } else {
                buf[..n - base].copy_from_slice(&data[base..]);
                buf[n - base..].fill(0);
                masks64(buf.as_ptr())
            }
        };
        let mut sep = mc | mn;
        let mut qrem = mq;
        while sep != 0 {
            let k = sep.trailing_zeros();
            sep &= sep - 1;
            let i = base + k as usize;
            let below = (1u64 << k) - 1;
            let idx = nf.min(N);
            ends[idx] = i;
            qn[idx] = qcarry + (qrem & below).count_ones();
            qrem &= !below;
            qcarry = 0;
            nf += 1;
            if (mn >> k) & 1 == 1 {
                if nf == N {
                    emit::<N>(data, ls, &ends, &qn, &mut f);
                }
                nf = 0;
                ls = i + 1;
            }
        }
        qcarry += qrem.count_ones();
        base += 64;
    }
    // last line without a trailing newline
    if ls < n {
        let idx = nf.min(N);
        ends[idx] = n;
        qn[idx] = qcarry;
        nf += 1;
        if nf == N {
            emit::<N>(data, ls, &ends, &qn, &mut f);
        }
    }
}

/// Scalar reference version of `scan` (kept for testing).
#[allow(dead_code)]
#[inline(always)]
fn scan_scalar<const N: usize>(data: &[u8], mut f: impl FnMut(&[&[u8]; N])) {
    let n = data.len();
    let mut i = 0;
    let mut out: [&[u8]; N] = [&[]; N];
    while i < n {
        let mut nf = 0usize;
        let mut fs = i;
        let mut bad = false;
        let mut q = false;
        loop {
            let c = if i < n { data[i] } else { b'\n' };
            if c == b',' || c == b'\n' {
                if nf < N {
                    let fld = &data[fs..i];
                    if q {
                        let l = fld.len();
                        if l >= 2
                            && fld[0] == b'"'
                            && fld[l - 1] == b'"'
                            && memchr(b'"', &fld[1..l - 1]).is_none()
                        {
                            out[nf] = &fld[1..l - 1];
                        } else {
                            bad = true;
                        }
                    } else {
                        out[nf] = fld;
                    }
                }
                nf += 1;
                q = false;
                fs = i + 1;
                if c == b'\n' {
                    break;
                }
            } else if c == b'"' {
                q = true;
            }
            i += 1;
        }
        i += 1;
        if !bad && nf == N {
            f(&out);
        }
    }
}

#[inline]
fn for_lines(data: &[u8], mut f: impl FnMut(&[u8])) {
    let mut s = 0;
    let n = data.len();
    while s < n {
        let e = match memchr(b'\n', &data[s..]) {
            Some(p) => s + p,
            None => n,
        };
        if e > s {
            f(&data[s..e]);
        }
        s = e + 1;
    }
}

// ---------------------------------------------------------------- time

const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

static MONTH_START: OnceLock<Vec<i32>> = OnceLock::new();
#[inline(always)]
fn month_start() -> &'static [i32] {
    MONTH_START.get_or_init(|| {
        let mut v = Vec::with_capacity(400 * 12);
        for y in 1900..2300 {
            for m in 1..=12 {
                v.push(days_from_civil(y, m, 1) as i32);
            }
        }
        v
    })
}

#[inline]
fn d2(a: u8, b: u8) -> Option<i64> {
    if a.is_ascii_digit() && b.is_ascii_digit() {
        Some(((a - b'0') * 10 + (b - b'0')) as i64)
    } else {
        None
    }
}

/// Days since epoch of the first of each month, 1900-01 .. 2300-01 (inclusive).
const MS_Y0: i64 = 1900;
const MS_N: usize = 400 * 12 + 1;
static MS: [i32; MS_N] = {
    let mut t = [0i32; MS_N];
    let mut i = 0;
    while i < MS_N {
        let y = MS_Y0 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        t[i] = days_from_civil(y, m, 1) as i32;
        i += 1;
    }
    t
};

/// Fast path for `YYYY-MM-DDTHH:MM:SS` (exactly 19 bytes available).
#[inline(always)]
fn civil(s: &[u8]) -> Option<i64> {
    let s: &[u8; 19] = s[..19].try_into().ok()?;
    let d = |i: usize| s[i].wrapping_sub(b'0') as u32;
    let bad = (d(0) > 9) | (d(1) > 9) | (d(2) > 9) | (d(3) > 9) | (d(5) > 9) | (d(6) > 9)
        | (d(8) > 9) | (d(9) > 9) | (d(11) > 9) | (d(12) > 9) | (d(14) > 9) | (d(15) > 9)
        | (d(17) > 9) | (d(18) > 9)
        | (s[4] != b'-') | (s[7] != b'-') | (s[10] != b'T') | (s[13] != b':') | (s[16] != b':');
    if bad {
        return None;
    }
    let year = (d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3)) as i64;
    let month = d(5) * 10 + d(6);
    let day = d(8) * 10 + d(9);
    let hour = d(11) * 10 + d(12);
    let minute = d(14) * 10 + d(15);
    let second = d(17) * 10 + d(18);
    if (month.wrapping_sub(1) > 11) | (hour > 23) | (minute > 59) | (second > 59) | (day == 0) {
        return None;
    }
    let days = if year >= MS_Y0 && year < MS_Y0 + 400 {
        let i = ((year - MS_Y0) * 12) as usize + month as usize - 1;
        let first = MS[i];
        if day as i32 > MS[i + 1] - first {
            return None;
        }
        first as i64 + day as i64 - 1
    } else {
        return civil_slow(year, month as i64, day as i64, hour as i64, minute as i64, second as i64);
    };
    Some(days * DAY + (hour * 3_600_000 + minute * 60_000 + second * 1000) as i64)
}

#[cold]
fn civil_slow(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> Option<i64> {
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let dim = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => if leap { 29 } else { 28 },
    };
    if day > dim {
        return None;
    }
    Some(days_from_civil(year, month, day) * DAY + hour * 3_600_000 + minute * 60_000 + second * 1000)
}

#[allow(dead_code)]
fn civil_old(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s[0], s[1])? * 100 + d2(s[2], s[3])?;
    let month = d2(s[5], s[6])?;
    let day = d2(s[8], s[9])?;
    let hour = d2(s[11], s[12])?;
    let minute = d2(s[14], s[15])?;
    let second = d2(s[17], s[18])?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 || day < 1 {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let dim = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap {
                29
            } else {
                28
            }
        }
    };
    if day > dim {
        return None;
    }
    let days = if (1900..2300).contains(&year) {
        month_start()[((year - 1900) * 12 + month - 1) as usize] as i64 + day - 1
    } else {
        days_from_civil(year, month, day)
    };
    Some(days * DAY + hour * 3_600_000 + minute * 60_000 + second * 1000)
}

const HUGE: i64 = i64::MAX / 4;

/// Shuffle masks that right-align the first n bytes of a 16-byte lane.
const SHUF: [[u8; 16]; 17] = {
    let mut t = [[0x80u8; 16]; 17];
    let mut n = 0;
    while n <= 16 {
        let mut j = 0;
        while j < 16 {
            if j >= 16 - n {
                t[n][j] = (j - (16 - n)) as u8;
            }
            j += 1;
        }
        n += 1;
    }
    t
};

/// Parses 1..=16 ASCII digits at `p` (16 bytes must be readable); None if any
/// of the first n bytes is not a digit.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn digits16(p: *const u8, n: usize) -> Option<u64> {
    use std::arch::x86_64::*;
    let v = _mm_loadu_si128(p as *const __m128i);
    let t = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
    let ok = _mm_cmpeq_epi8(_mm_min_epu8(t, _mm_set1_epi8(9)), t);
    let m = _mm_movemask_epi8(ok) as u32;
    let need = (1u32 << n) - 1;
    if m & need != need {
        return None;
    }
    let a = _mm_shuffle_epi8(t, _mm_loadu_si128(SHUF[n].as_ptr() as *const __m128i));
    let m1 = _mm_maddubs_epi16(a, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
    let m2 = _mm_madd_epi16(m1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
    let m3 = _mm_packus_epi32(m2, m2);
    let m4 = _mm_madd_epi16(m3, _mm_setr_epi16(10000, 1, 10000, 1, 10000, 1, 10000, 1));
    let hi = _mm_cvtsi128_si32(m4) as u32 as u64;
    let lo = _mm_extract_epi32(m4, 1) as u32 as u64;
    Some(hi * 100_000_000 + lo)
}

/// `ingest_ts` is always valid F1 per the rules; parse it without validation
/// when it has the F1 shape, else fall back to the full parser.
#[inline(always)]
fn parse_ingest(s: &[u8]) -> i64 {
    if s.len() == 20 && s[19] == b'Z' {
        let d = |i: usize| (s[i].wrapping_sub(b'0')) as i64;
        let year = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
        let month = d(5) * 10 + d(6);
        if (MS_Y0..MS_Y0 + 400).contains(&year) && (1..=12).contains(&month) {
            let i = ((year - MS_Y0) * 12 + month - 1) as usize;
            let day = d(8) * 10 + d(9);
            let secs = (d(11) * 10 + d(12)) * 3600 + (d(14) * 10 + d(15)) * 60 + d(17) * 10 + d(18);
            return (MS[i] as i64 + day - 1) * DAY + secs * 1000;
        }
    }
    parse_ts(s).unwrap_or(i64::MAX / 2)
}

#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return civil(s);
    }
    if n == 25 && s[22] == b':' && (s[19] == b'+' || s[19] == b'-') {
        let base = civil(s)?;
        let oh = d2(s[20], s[21])?;
        let om = d2(s[23], s[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60_000;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    if n == 0 {
        return None;
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        let p = s.as_ptr();
        if n <= 16 && (p as usize & 4095) <= 4096 - 16 {
            return unsafe { digits16(p, n) }.map(|v| v as i64);
        }
    }
    // F3: ASCII digits only, any length; huge values saturate (outside every window)
    let mut v: i64 = 0;
    let mut big = false;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if v > 100_000_000_000_000_000 {
            big = true;
        } else {
            v = v * 10 + d as i64;
        }
    }
    Some(if big || v > 100_000_000_000_000_000 { HUGE } else { v })
}

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
        if v > 10_000_000 {
            v = 10_000_001; // keep checking digits, value already too big
        }
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

// ---------------------------------------------------------------- records

// Times inside records are u32 milliseconds since `tb = window_start - 7 days`.
#[derive(Clone, Copy)]
#[repr(C, packed(8))]
struct ERaw {
    eid: Key,
    user: Key,
    ingest: i32, // seconds relative to window_start, clamped; only orders copies
    ts: u32,
    price: u32,
    cat: u16,
    kind: u8, // 0 invalid, 1 view, 2 cart, 3 purchase
}

#[derive(Clone, Copy)]
#[repr(C, packed(8))]
struct CRaw {
    cid: Key,
    user: Key,
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
#[repr(C, packed(8))]
struct PEv {
    person: Key,
    ts: u32,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
#[repr(C, packed(8))]
struct PC {
    person: Key,
    cid: Key,
    ts: u32,
    camp: u32,
}

struct UserRow {
    person: Key,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u16, // u16::MAX = null
}

struct Ctx {
    tb: i64,
    ws: i64,
    we: i64,
    products: KMap<u16>,
    products_long: std::collections::HashMap<Vec<u8>, u16>,
    unknown_cat: u16,
    camps: KMap<u32>,
    camps_long: std::collections::HashMap<Vec<u8>, u32>,
}

impl Ctx {
    #[inline]
    fn cat(&self, p: &[u8]) -> u16 {
        match compact(p) {
            Some(k) => *self.products.get(&k).unwrap_or(&self.unknown_cat),
            None => *self.products_long.get(p).unwrap_or(&self.unknown_cat),
        }
    }
    #[inline]
    fn camp(&self, c: &[u8]) -> Option<u32> {
        match compact(c) {
            Some(k) => self.camps.get(&k).copied(),
            None => self.camps_long.get(c).copied(),
        }
    }
}

#[inline]
fn parse_event(f: &[&[u8]; 7], ctx: &Ctx) -> Option<ERaw> {
    if f[0].is_empty() {
        return None;
    }
    let ingest = parse_ingest(f[3]);
    let mut r = ERaw {
        eid: pack(f[0]),
        user: 0,
        ingest: ((ingest - ctx.ws).div_euclid(1000)).clamp(i32::MIN as i64, i32::MAX as i64) as i32,
        ts: 0,
        price: 0,
        cat: 0,
        kind: 0,
    };
    if f[1].is_empty() || f[5].is_empty() {
        return Some(r);
    }
    let t = f[4];
    let kind = if t.len() == 4 {
        let w = u32::from_le_bytes([t[0], t[1], t[2], t[3]]);
        if w == u32::from_le_bytes(*b"view") {
            1
        } else if w == u32::from_le_bytes(*b"cart") {
            2
        } else {
            return Some(r);
        }
    } else if t.len() == 8 && u64::from_le_bytes(t[..8].try_into().unwrap()) == u64::from_le_bytes(*b"purchase") {
        3
    } else {
        return Some(r);
    };
    let ts = match parse_ts(f[2]) {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => return Some(r),
    };
    if kind == 3 {
        match parse_price(f[6]) {
            Some(p) => r.price = p,
            None => return Some(r),
        }
    }
    if ingest.saturating_sub(ts) > DAY {
        return Some(r);
    }
    if kind == 3 {
        r.cat = ctx.cat(f[5]);
    }
    r.user = pack(f[1]);
    r.ts = (ts - ctx.tb) as u32;
    r.kind = kind;
    Some(r)
}

#[inline]
fn parse_click(f: &[&[u8]; 4], ctx: &Ctx) -> Option<CRaw> {
    if f[0].is_empty() || f[1].is_empty() {
        return None;
    }
    let ts = parse_ts(f[3])?;
    if ts < ctx.ws - WEEK || ts >= ctx.we {
        return None;
    }
    let camp = ctx.camp(f[2])?;
    Some(CRaw {
        cid: pack(f[0]),
        user: pack(f[1]),
        ts: (ts - ctx.tb) as u32,
        camp,
    })
}

// ---------------------------------------------------------------- parallel helper

fn par_tasks<S: Send>(
    n_tasks: usize,
    nthreads: usize,
    init: impl Fn() -> S + Sync,
    f: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads.max(1))
            .map(|_| {
                sc.spawn(|| {
                    let mut st = init();
                    loop {
                        let t = next.fetch_add(1, Ordering::Relaxed);
                        if t >= n_tasks {
                            break;
                        }
                        f(&mut st, t);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- input

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Ev,
    Ck,
    Lk,
}

/// A raw byte region of a file. The region owns every line that starts inside
/// it (the header line of the file is skipped).
struct Chunk {
    kind: Kind,
    file: usize,
    start: u64,
    end: u64,
}

const EXTRA: usize = 1 << 16;

fn read_at(f: &fs::File, buf: &mut [u8], off: u64) -> usize {
    use std::os::unix::fs::FileExt;
    let mut got = 0;
    while got < buf.len() {
        match f.read_at(&mut buf[got..], off + got as u64) {
            Ok(0) => break,
            Ok(k) => got += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read: {e}"),
        }
    }
    got
}

/// Loads the lines owned by `c` into `buf`; returns the byte range in `buf`.
fn load_region(f: &fs::File, flen: u64, c: &Chunk, buf: &mut Vec<u8>) -> (usize, usize) {
    let rs = if c.start == 0 { 0 } else { c.start - 1 };
    let mut want = ((c.end - rs) as usize + EXTRA).min((flen - rs) as usize);
    loop {
        if buf.len() < want + 64 {
            buf.resize(want + 64, 0);
        }
        let got = read_at(f, &mut buf[..want], rs);
        let data = &buf[..got];
        let first = if c.start == 0 {
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => return (0, 0),
            }
        } else if data.first() == Some(&b'\n') {
            1
        } else {
            match memchr(b'\n', data) {
                Some(p) => p + 1,
                None => return (0, 0),
            }
        };
        let rel_end = (c.end - rs) as usize;
        if first >= rel_end {
            return (0, 0);
        }
        match memchr(b'\n', &data[rel_end - 1..]) {
            Some(p) => return (first, rel_end - 1 + p + 1),
            None => {
                if rs + got as u64 >= flen {
                    return (first, got);
                }
                want = (want * 2).min((flen - rs) as usize);
            }
        }
    }
}

fn read_small_csv(p: &Path, mut f: impl FnMut([&[u8]; 2])) {
    let data = fs::read(p).unwrap_or_default();
    let Some(h) = memchr(b'\n', &data) else { return };
    for_lines(&data[h + 1..], |line| {
        if let Some(fs) = split::<2>(line) {
            f(fs);
        }
    });
}

fn manifest_value(text: &str, key: &str) -> String {
    let i = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[i + key.len() + 2..];
    let c = rest.find(':').unwrap();
    let rest = &rest[c + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    rest[..q2].to_string()
}

// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let stop_after: u32 = std::env::var("STOP_AFTER").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest");
    let ws = parse_ts(manifest_value(&manifest, "window_start").as_bytes()).expect("ws");
    let we = parse_ts(manifest_value(&manifest, "window_end").as_bytes()).expect("we");

    // categories and products
    let mut prod_raw: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    read_small_csv(&data.join("products.csv"), |f| {
        prod_raw.push((f[0].to_vec(), f[1].to_vec()));
    });
    let mut cats: Vec<Vec<u8>> = prod_raw.iter().map(|(_, c)| c.clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let cat_idx: std::collections::HashMap<Vec<u8>, u16> =
        cats.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let unknown_cat = cat_idx[&b"unknown"[..]];
    let mut products: KMap<u16> = KMap::with_capacity_and_hasher(prod_raw.len() * 2, KH);
    let mut products_long = std::collections::HashMap::new();
    for (p, c) in &prod_raw {
        let ci = cat_idx[c];
        match compact(p) {
            Some(k) => {
                products.insert(k, ci);
            }
            None => {
                products_long.insert(p.clone(), ci);
            }
        }
    }

    // campaigns
    let mut camp_ids: Vec<Vec<u8>> = Vec::new();
    let mut camp_channels: Vec<Vec<u8>> = Vec::new();
    read_small_csv(&data.join("campaigns.csv"), |f| {
        camp_ids.push(f[0].to_vec());
        camp_channels.push(f[1].to_vec());
    });
    let ncamp = camp_ids.len();
    let mut camps: KMap<u32> = KMap::with_capacity_and_hasher(ncamp * 2, KH);
    let mut camps_long = std::collections::HashMap::new();
    for (i, c) in camp_ids.iter().enumerate() {
        match compact(c) {
            Some(k) => {
                camps.insert(k, i as u32);
            }
            None => {
                camps_long.insert(c.clone(), i as u32);
            }
        }
    }

    assert!(we - ws + WEEK < u32::MAX as i64, "window too long");
    let ctx = Ctx {
        tb: ws - WEEK,
        ws,
        we,
        products,
        products_long,
        unknown_cat,
        camps,
        camps_long,
    };

    // open input files and cut them into regions
    let mut files: Vec<(fs::File, u64)> = Vec::new();
    let mut chunks: Vec<Chunk> = Vec::new();
    let (mut ev_bytes, mut ck_bytes) = (0usize, 0usize);
    for (sub, kind) in [("links", Kind::Lk), ("events", Kind::Ev), ("clicks", Kind::Ck)] {
        for p in list_csv(&data.join(sub)) {
            let Ok(f) = fs::File::open(&p) else { continue };
            let flen = f.metadata().map(|m| m.len()).unwrap_or(0);
            if flen == 0 {
                continue;
            }
            match kind {
                Kind::Ev => ev_bytes += flen as usize,
                Kind::Ck => ck_bytes += flen as usize,
                Kind::Lk => {}
            }
            let fi = files.len();
            let mut st = 0u64;
            while st < flen {
                let en = (st + CHUNK as u64).min(flen);
                chunks.push(Chunk { kind, file: fi, start: st, end: en });
                st = en;
            }
            files.push((f, flen));
        }
    }
    let ev_cap = ev_bytes / 60 / nthreads / NB * 5 / 4 + 16;
    let ck_cap = ck_bytes / 40 / nthreads * 5 / 4 + 16;
    eprintln!("[{:?}] {} chunks", t0.elapsed(), chunks.len());

    // ---------------- phase A: parse
    struct AState {
        ev: Vec<Vec<ERaw>>,
        ck: Vec<CRaw>,
        lk: Vec<(Key, Key)>,
        buf: Vec<u8>,
    }
    // links first (their chunks lead the list), then union-find on its own
    // thread while events and clicks parse
    let nlk = chunks.iter().take_while(|c| c.kind == Kind::Lk).count();
    let lstates = par_tasks(
        nlk,
        nthreads,
        || (Vec::<(Key, Key)>::new(), Vec::<u8>::new()),
        |st, t| {
            let c = &chunks[t];
            let (f, flen) = &files[c.file];
            let (lo, hi) = load_region(f, *flen, c, &mut st.1);
            let out = &mut st.0;
            scan::<2>(&st.1[lo..hi], |f| {
                if !f[0].is_empty() && !f[1].is_empty() {
                    out.push((pack(f[0]), pack(f[1])));
                }
            });
        },
    );
    let (persons, astates) = std::thread::scope(|sc| {
    let ph = sc.spawn(|| {
        let p = build_persons(&lstates.iter().map(|s| &s.0[..]).collect::<Vec<_>>());
        eprintln!("[{:?}] persons map {} ids", t0.elapsed(), p.len());
        p
    });
    let astates = par_tasks(
        chunks.len() - nlk,
        nthreads,
        || AState {
            ev: (0..NB).map(|_| Vec::with_capacity(ev_cap)).collect(),
            ck: Vec::with_capacity(ck_cap),
            lk: Vec::new(),
            buf: Vec::new(),
        },
        |st, t| {
            let c = &chunks[nlk + t];
            let (f, flen) = &files[c.file];
            let mut buf = std::mem::take(&mut st.buf);
            let (lo, hi) = load_region(f, *flen, c, &mut buf);
            let bytes = &buf[lo..hi];
            match c.kind {
                Kind::Ev => scan::<7>(bytes, |f| {
                    if let Some(r) = parse_event(f, &ctx) {
                        st.ev[bucket_of(r.eid)].push(r);
                    }
                }),
                Kind::Ck => scan::<4>(bytes, |f| {
                    if let Some(r) = parse_click(f, &ctx) {
                        st.ck.push(r);
                    }
                }),
                Kind::Lk => scan::<2>(bytes, |f| {
                    if !f[0].is_empty() && !f[1].is_empty() {
                        st.lk.push((pack(f[0]), pack(f[1])));
                    }
                }),
            }
            st.buf = buf;
        },
    );
    eprintln!("[{:?}] parsed", t0.elapsed());
    if stop_after == 1 { std::process::exit(0); }
    (ph.join().unwrap(), astates)
    });
    drop(lstates);

    // ---------------- phase B: dedup events, bucket by person
    struct BState {
        ev: Vec<Vec<PEv>>,
        ck: Vec<Vec<PC>>,
        buf: Vec<u64>,
        dead: Vec<u64>,
    }
    let nck_tasks = astates.len();
    assert!(nthreads <= 64);
    let bstates = par_tasks(
        NB + nck_tasks,
        nthreads,
        || BState {
            ev: (0..NB).map(|_| Vec::with_capacity(ev_cap)).collect(),
            ck: (0..NB).map(|_| Vec::with_capacity(ck_cap / NB + 16)).collect(),
            buf: Vec::new(),
            dead: Vec::new(),
        },
        |st, t| {
            if t < nck_tasks {
                let ck = &mut st.ck;
                batched(&persons, astates[t].ck.iter().copied(), |c| c.user, |c, p| {
                    ck[bucket_of(p)].push(PC {
                        person: p,
                        cid: c.cid,
                        ts: c.ts,
                        camp: c.camp,
                    });
                });
                return;
            }
            let b = t - nck_tasks;
            // open-addressing table over (thread, index) refs; keep min ingest per eid
            let total: usize = astates.iter().map(|a| a.ev[b].len()).sum();
            let cap = (total * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let table = &mut st.buf;
            table.clear();
            table.resize(cap, u64::MAX);
            // entry = fingerprint(32) | thread(6) | index(26); u64::MAX = empty
            let at = |e: u64| -> &ERaw {
                let r = e as u32;
                &astates[(r >> 26) as usize].ev[b][(r & 0x3FF_FFFF) as usize]
            };
            // dead[i] marks records that lost dedup; i = offs[ti] + ri
            let mut offs = [0usize; 65];
            for (ti, a) in astates.iter().enumerate() {
                offs[ti + 1] = offs[ti] + a.ev[b].len();
            }
            let dead = &mut st.dead;
            dead.clear();
            dead.resize(total / 64 + 1, 0);
            let kill = |e: u64, dead: &mut Vec<u64>| {
                let r = e as u32;
                let i = offs[(r >> 26) as usize] + (r & 0x3FF_FFFF) as usize;
                dead[i >> 6] |= 1u64 << (i & 63);
            };
            for (ti, a) in astates.iter().enumerate() {
                let v = &a.ev[b];
                for (ri, r) in v.iter().enumerate() {
                    let eid = r.eid;
                    let hm = mix(eid);
                    let fp = hm >> 32;
                    let me = (fp << 32) | ((ti as u64) << 26) | ri as u64;
                    let mut h = (hm >> 12) as usize & mask;
                    loop {
                        let e = table[h];
                        if e == u64::MAX {
                            table[h] = me;
                            break;
                        }
                        if e >> 32 == fp {
                            let o = at(e);
                            let oe = o.eid;
                            if oe == eid {
                                let (oi, ni) = (o.ingest, r.ingest);
                                if ni < oi {
                                    kill(e, dead);
                                    table[h] = me;
                                } else {
                                    kill(me, dead);
                                }
                                break;
                            }
                        }
                        h = (h + 1) & mask;
                    }
                }
            }
            let evb = &mut st.ev;
            let dead = &st.dead;
            let survivors = astates
                .iter()
                .enumerate()
                .flat_map(|(ti, a)| {
                    let o = offs[ti];
                    a.ev[b].iter().enumerate().filter(move |(ri, _)| {
                        let i = o + ri;
                        dead[i >> 6] >> (i & 63) & 1 == 0
                    })
                })
                .map(|(_, r)| *r)
                .filter(|r| r.kind != 0);
            batched(&persons, survivors, |r| r.user, |r, p| {
                evb[bucket_of(p)].push(PEv {
                    person: p,
                    ts: r.ts,
                    price: r.price,
                    cat: r.cat,
                    purchase: r.kind == 3,
                });
            });
        },
    );
    drop(astates);
    eprintln!("[{:?}] deduped", t0.elapsed());
    if stop_after == 2 { std::process::exit(0); }

    // ---------------- phase C: sessions
    let ndays = (((we - 1).div_euclid(DAY)) - ws.div_euclid(DAY) + 1).max(1) as usize;
    let day0 = ws.div_euclid(DAY);
    let ncat = cats.len();
    let tb = ws - WEEK;
    let cstates = par_tasks(
        NB,
        nthreads,
        || CState {
            rows: Vec::new(),
            stats: vec![[0u64; 4]; (ncamp + 1) * ndays],
            cnt: vec![0; ncamp],
            pos: vec![0; ncamp],
            active: Vec::new(),
            catsum: vec![0; ncat],
            evbuf: Vec::new(),
            ckbuf: Vec::new(),
            lmap: LocalMap::default(),
            lid: Vec::new(),
            estart: Vec::new(),
            cstart: Vec::new(),
        },
        |st, b| {
            // Group by person without a full sort: local dense ids via a small
            // hash table, counting sort by id, then sort each person by time.
            let mut evs = std::mem::take(&mut st.evbuf);
            let mut cks = std::mem::take(&mut st.ckbuf);
            let mut lm = std::mem::take(&mut st.lmap);
            let mut lid = std::mem::take(&mut st.lid);
            let mut start = std::mem::take(&mut st.estart);
            let mut cstart = std::mem::take(&mut st.cstart);
            let total_e: usize = bstates.iter().map(|s| s.ev[b].len()).sum();
            lm.reset((total_e / 8).max(64));
            lid.clear();
            for s in &bstates {
                for e in s.ev[b].iter() {
                    lid.push(lm.get_or_insert(e.person));
                }
            }
            let np = lm.keys.len();
            start.clear();
            start.resize(np + 1, 0);
            for &l in &lid {
                start[l as usize + 1] += 1;
            }
            for i in 0..np {
                start[i + 1] += start[i];
            }
            evs.clear();
            evs.reserve(total_e);
            unsafe { evs.set_len(total_e) };
            {
                let mut pos: Vec<u32> = start[..np].to_vec();
                let mut k = 0;
                for s in &bstates {
                    for e in s.ev[b].iter() {
                        let l = lid[k] as usize;
                        k += 1;
                        evs[pos[l] as usize] = *e;
                        pos[l] += 1;
                    }
                }
            }
            // clicks of persons that have events
            lid.clear();
            cks.clear();
            let mut ccount = vec![0u32; np + 1];
            for s in &bstates {
                for c in s.ck[b].iter() {
                    match lm.get(c.person) {
                        Some(l) => {
                            lid.push(l);
                            ccount[l as usize + 1] += 1;
                        }
                        None => lid.push(u32::MAX),
                    }
                }
            }
            cstart.clear();
            cstart.extend_from_slice(&ccount);
            for i in 0..np {
                cstart[i + 1] += cstart[i];
            }
            let total_c = cstart[np] as usize;
            cks.reserve(total_c);
            unsafe { cks.set_len(total_c) };
            {
                let mut pos: Vec<u32> = cstart[..np].to_vec();
                let mut k = 0;
                for s in &bstates {
                    for c in s.ck[b].iter() {
                        let l = lid[k];
                        k += 1;
                        if l != u32::MAX {
                            cks[pos[l as usize] as usize] = *c;
                            pos[l as usize] += 1;
                        }
                    }
                }
            }
            for l in 0..np {
                let es = &mut evs[start[l] as usize..start[l + 1] as usize];
                if es.len() > 1 {
                    es.sort_unstable_by_key(|x| x.ts);
                }
                let cs = &mut cks[cstart[l] as usize..cstart[l + 1] as usize];
                if cs.len() > 1 {
                    cs.sort_unstable_by(|a, b| {
                        let (at, bt, ac, bc) = (a.ts, b.ts, a.cid, b.cid);
                        at.cmp(&bt).then_with(|| key_cmp(ac, bc))
                    });
                }
                process_person(lm.keys[l], es, cs, st.view(), ncamp, ndays, day0, tb);
            }
            st.evbuf = evs;
            st.ckbuf = cks;
            st.lmap = lm;
            st.lid = lid;
            st.estart = start;
            st.cstart = cstart;
        },
    );
    drop(bstates);
    eprintln!("[{:?}] sessionized", t0.elapsed());
    if stop_after == 3 { std::process::exit(0); }

    // ---------------- phase D: write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();

    let mut stats = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for s in &cstates {
        for (a, b) in stats.iter_mut().zip(s.stats.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
    }
    let cats = Arc::new(cats);
    std::thread::scope(|sc| {
        for (ti, s) in cstates.iter().enumerate() {
            let ud = ud.clone();
            let cats = cats.clone();
            sc.spawn(move || write_users(&ud.join(format!("part-{ti:04}.parquet")), &s.rows, &cats));
        }
        write_campaigns(
            &cd.join("part-0000.parquet"),
            &stats,
            &camp_ids,
            &camp_channels,
            ncamp,
            ndays,
            day0,
        );
    });
    eprintln!("[{:?}] done", t0.elapsed());
    // skip freeing gigabytes of buffers at shutdown
    std::process::exit(0);
}

struct StView<'a> {
    rows: &'a mut Vec<UserRow>,
    stats: &'a mut [[u64; 4]],
    cnt: &'a mut [u32],
    pos: &'a mut [u32],
    active: &'a mut Vec<u32>,
    catsum: &'a mut [u64],
}

struct CState {
    rows: Vec<UserRow>,
    stats: Vec<[u64; 4]>, // sessions, purchasing, revenue, assisted
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    catsum: Vec<u64>,
    evbuf: Vec<PEv>,
    ckbuf: Vec<PC>,
    lmap: LocalMap,
    lid: Vec<u32>,
    estart: Vec<u32>,
    cstart: Vec<u32>,
}

/// Small growable open-addressing map person key -> dense local id.
#[derive(Default)]
struct LocalMap {
    mask: usize,
    slots: Vec<(Key, u32)>,
    keys: Vec<Key>,
}

impl LocalMap {
    fn reset(&mut self, hint: usize) {
        let cap = (hint * 2).next_power_of_two().max(64);
        self.mask = cap - 1;
        self.slots.clear();
        self.slots.resize(cap, (0, u32::MAX));
        self.keys.clear();
    }
    #[inline(always)]
    fn h(&self, k: Key) -> usize {
        (mix(k) >> 17) as usize & self.mask
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: Key) -> u32 {
        let mut h = self.h(k);
        loop {
            let (sk, sv) = self.slots[h];
            if sv == u32::MAX {
                let id = self.keys.len() as u32;
                self.slots[h] = (k, id);
                self.keys.push(k);
                if self.keys.len() * 2 > self.slots.len() {
                    self.grow();
                }
                return id;
            }
            if sk == k {
                return sv;
            }
            h = (h + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: Key) -> Option<u32> {
        let mut h = self.h(k);
        loop {
            let (sk, sv) = self.slots[h];
            if sv == u32::MAX {
                return None;
            }
            if sk == k {
                return Some(sv);
            }
            h = (h + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let cap = self.slots.len() * 2;
        self.mask = cap - 1;
        self.slots.clear();
        self.slots.resize(cap, (0, u32::MAX));
        for (id, &k) in self.keys.iter().enumerate() {
            let mut h = (mix(k) >> 17) as usize & self.mask;
            while self.slots[h].1 != u32::MAX {
                h = (h + 1) & self.mask;
            }
            self.slots[h] = (k, id as u32);
        }
    }
}

impl CState {
    fn view(&mut self) -> StView<'_> {
        StView {
            rows: &mut self.rows,
            stats: &mut self.stats,
            cnt: &mut self.cnt,
            pos: &mut self.pos,
            active: &mut self.active,
            catsum: &mut self.catsum,
        }
    }
}

fn process_person(
    person: Key,
    evs: &[PEv],
    cks: &[PC],
    st: StView<'_>,
    ncamp: usize,
    ndays: usize,
    day0: i64,
    tb: i64,
) {
    let StView {
        rows,
        stats,
        cnt,
        pos,
        active,
        catsum,
    } = st;
    let organic = ncamp;
    let nc = cks.len();
    let (mut lo, mut hi) = (0usize, 0usize);

    let mut cur_day: i64 = i64::MIN;
    let (mut g_sessions, mut g_events, mut g_rev) = (0u32, 0u32, 0u64);

    let flush = |rows: &mut Vec<UserRow>, catsum: &mut [u64], day: i64, s: u32, e: u32, r: u64| {
        if s == 0 {
            return;
        }
        let mut top = u16::MAX;
        if r > 0 {
            let mut best = 0u64;
            for (i, v) in catsum.iter_mut().enumerate() {
                if *v > best {
                    best = *v;
                    top = i as u16;
                }
                *v = 0;
            }
        }
        rows.push(UserRow {
            person,
            day: day as i32,
            sessions: s,
            events: e,
            revenue: r,
            top,
        });
    };

    let n = evs.len();
    let mut k = 0;
    while k < n {
        let s = evs[k].ts as i64;
        let d = (s + tb).div_euclid(DAY);
        if d != cur_day {
            flush(rows, catsum, cur_day, g_sessions, g_events, g_rev);
            cur_day = d;
            g_sessions = 0;
            g_events = 0;
            g_rev = 0;
        }
        let mut last = s;
        let mut m = k;
        let mut srev = 0u64;
        let mut has_p = false;
        while m < n && evs[m].ts as i64 - last <= GAP {
            let e = evs[m];
            last = e.ts as i64;
            if e.purchase {
                srev += e.price as u64;
                catsum[e.cat as usize] += e.price as u64;
                has_p = true;
            }
            m += 1;
        }
        g_sessions += 1;
        g_events += (m - k) as u32;
        g_rev += srev;

        // attribution window [s - WEEK, s]
        while hi < nc && cks[hi].ts as i64 <= s {
            let c = cks[hi].camp as usize;
            if cnt[c] == 0 {
                pos[c] = active.len() as u32;
                active.push(c as u32);
            }
            cnt[c] += 1;
            hi += 1;
        }
        while lo < hi && (cks[lo].ts as i64) < s - WEEK {
            let c = cks[lo].camp as usize;
            cnt[c] -= 1;
            if cnt[c] == 0 {
                let p = pos[c] as usize;
                let last_c = *active.last().unwrap();
                active[p] = last_c;
                pos[last_c as usize] = p as u32;
                active.pop();
            }
            lo += 1;
        }
        let credited = if hi > lo {
            let t = cks[hi - 1].ts;
            let mut q = hi - 1;
            while q > lo && { cks[q - 1].ts } == t {
                q -= 1;
            }
            cks[q].camp as usize
        } else {
            organic
        };
        let di = (d - day0) as usize;
        debug_assert!(di < ndays);
        let row = &mut stats[credited * ndays + di];
        row[0] += 1;
        row[1] += has_p as u64;
        row[2] += srev;
        for &c in active.iter() {
            stats[c as usize * ndays + di][3] += 1;
        }
        k = m;
    }
    flush(rows, catsum, cur_day, g_sessions, g_events, g_rev);
    for &c in active.iter() {
        cnt[c as usize] = 0;
    }
    active.clear();
}

/// id -> person name for linked ids; open addressing, key 0 = empty slot
/// (0 is the empty string, never a linked id). Supports prefetching.
struct PTable {
    mask: usize,
    slots: Vec<[Key; 2]>,
}

impl PTable {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        PTable { mask: cap - 1, slots: vec![[0, 0]; cap] }
    }
    #[inline(always)]
    fn slot(&self, k: Key) -> usize {
        (mix(k) >> 11) as usize & self.mask
    }
    fn insert(&mut self, k: Key, v: Key) {
        let mut h = self.slot(k);
        loop {
            if self.slots[h][0] == 0 || self.slots[h][0] == k {
                self.slots[h] = [k, v];
                return;
            }
            h = (h + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, k: Key) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.slots.as_ptr().add(self.slot(k)) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn get(&self, k: Key) -> Key {
        let mut h = self.slot(k);
        loop {
            let s = &self.slots[h];
            if s[0] == k {
                return s[1];
            }
            if s[0] == 0 {
                return k;
            }
            h = (h + 1) & self.mask;
        }
    }
    fn len(&self) -> usize {
        self.slots.iter().filter(|s| s[0] != 0).count()
    }
}

/// Resolves persons for a stream of items in prefetched batches.
#[inline(always)]
fn batched<T: Copy>(pt: &PTable, items: impl Iterator<Item = T>, user: impl Fn(&T) -> Key, mut emit: impl FnMut(T, Key)) {
    const B: usize = 16;
    let mut buf: [Option<T>; B] = [None; B];
    let mut n = 0;
    for it in items {
        pt.prefetch(user(&it));
        buf[n] = Some(it);
        n += 1;
        if n == B {
            for x in buf.iter_mut() {
                let v = x.take().unwrap();
                let p = pt.get(user(&v));
                emit(v, p);
            }
            n = 0;
        }
    }
    for x in buf[..n].iter_mut() {
        let v = x.take().unwrap();
        let p = pt.get(user(&v));
        emit(v, p);
    }
}

fn build_persons(parts: &[&[(Key, Key)]]) -> PTable {
    let total: usize = parts.iter().map(|p| p.len()).sum();
    let mut idx: KMap<u32> = KMap::with_capacity_and_hasher(total + total / 2 + 16, KH);
    let mut keys: Vec<Key> = Vec::with_capacity(total);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let g = parent[parent[x as usize] as usize];
            parent[x as usize] = g;
            x = g;
        }
        x
    }
    let mut get = |k: Key, keys: &mut Vec<Key>, parent: &mut Vec<u32>| -> u32 {
        *idx.entry(k).or_insert_with(|| {
            let i = keys.len() as u32;
            keys.push(k);
            parent.push(i);
            i
        })
    };
    for part in parts {
        for &(a, b) in part.iter() {
            let ia = get(a, &mut keys, &mut parent);
            let ib = get(b, &mut keys, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // union by smaller key root stays simple: attach larger index under smaller
                if ra < rb {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    let n = keys.len();
    let mut name: Vec<Key> = vec![u64::MAX; n];
    let mut roots = vec![0u32; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32);
        roots[i] = r;
        let cur = name[r as usize];
        if cur == u64::MAX || key_cmp(keys[i], cur) == std::cmp::Ordering::Less {
            name[r as usize] = keys[i];
        }
    }
    let mut out = PTable::new(n);
    for i in 0..n {
        let nm = name[roots[i] as usize];
        if nm != keys[i] {
            out.insert(keys[i], nm);
        }
    }
    out
}

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_max_row_group_size(1 << 20)
        .build()
}

fn to_string(b: Vec<u8>) -> String {
    String::from_utf8(b).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn write_users(path: &Path, rows: &[UserRow], cats: &[Vec<u8>]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let file = fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema.clone(), Some(props())).unwrap();
    let cat_s: Vec<String> = cats.iter().map(|c| to_string(c.clone())).collect();
    for part in rows.chunks(1 << 20).chain(if rows.is_empty() { Some(&rows[..]) } else { None }) {
        let uid: StringArray = part.iter().map(|r| Some(to_string(unpack(r.person)))).collect();
        let day: Date32Array = part.iter().map(|r| Some(r.day)).collect();
        let ses: Int64Array = part.iter().map(|r| Some(r.sessions as i64)).collect();
        let ev: Int64Array = part.iter().map(|r| Some(r.events as i64)).collect();
        let rev: Int64Array = part.iter().map(|r| Some(r.revenue as i64)).collect();
        let top: StringArray = part
            .iter()
            .map(|r| if r.top == u16::MAX { None } else { Some(cat_s[r.top as usize].as_str()) })
            .collect();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(uid),
            Arc::new(day),
            Arc::new(ses),
            Arc::new(ev),
            Arc::new(rev),
            Arc::new(top),
        ];
        let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
        w.write(&batch).unwrap();
    }
    w.close().unwrap();
}

fn write_campaigns(
    path: &Path,
    stats: &[[u64; 4]],
    ids: &[Vec<u8>],
    channels: &[Vec<u8>],
    ncamp: usize,
    ndays: usize,
    day0: i64,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let (mut cid, mut ch, mut day, mut s, mut ps, mut rev, mut asst) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        for d in 0..ndays {
            let r = stats[c * ndays + d];
            if r[0] == 0 && r[3] == 0 {
                continue;
            }
            if c == ncamp {
                cid.push("organic".to_string());
                ch.push("organic".to_string());
            } else {
                cid.push(to_string(ids[c].clone()));
                ch.push(to_string(channels[c].clone()));
            }
            day.push((day0 + d as i64) as i32);
            s.push(r[0] as i64);
            ps.push(r[1] as i64);
            rev.push(r[2] as i64);
            asst.push(if c == ncamp { 0 } else { r[3] as i64 });
        }
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(cid)),
        Arc::new(StringArray::from(ch)),
        Arc::new(Date32Array::from(day)),
        Arc::new(Int64Array::from(s)),
        Arc::new(Int64Array::from(ps)),
        Arc::new(Int64Array::from(rev)),
        Arc::new(Int64Array::from(asst)),
    ];
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let file = fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(file, schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Straightforward scalar reading of the timestamp rules.
    fn ts_ref(s: &[u8]) -> Option<i64> {
        if s.is_empty() {
            return None;
        }
        if s.iter().all(|c| c.is_ascii_digit()) {
            let mut v: i128 = 0;
            for &c in s {
                v = (v * 10 + (c - b'0') as i128).min(1 << 100);
            }
            return Some(if v > 100_000_000_000_000_000 { HUGE } else { v as i64 });
        }
        let civ = |b: &[u8]| -> Option<i64> {
            let st = std::str::from_utf8(b).ok()?;
            let bs = st.as_bytes();
            if bs.len() != 19 || bs[4] != b'-' || bs[7] != b'-' || bs[10] != b'T' || bs[13] != b':' || bs[16] != b':' {
                return None;
            }
            let num = |r: std::ops::Range<usize>| -> Option<i64> {
                if bs[r.clone()].iter().all(|c| c.is_ascii_digit()) { st[r].parse().ok() } else { None }
            };
            let (y, mo, d, h, mi, se) = (num(0..4)?, num(5..7)?, num(8..10)?, num(11..13)?, num(14..16)?, num(17..19)?);
            if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
                return None;
            }
            let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            let dim = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo as usize - 1];
            if d > dim {
                return None;
            }
            Some(days_from_civil(y, mo, d) * DAY + h * 3_600_000 + mi * 60_000 + se * 1000)
        };
        if s.len() == 20 && s[19] == b'Z' {
            return civ(&s[..19]);
        }
        if s.len() == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
            let b = civ(&s[..19])?;
            if !s[20..22].iter().chain(&s[23..25]).all(|c| c.is_ascii_digit()) {
                return None;
            }
            let oh = ((s[20] - b'0') * 10 + (s[21] - b'0')) as i64;
            let om = ((s[23] - b'0') * 10 + (s[24] - b'0')) as i64;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            return Some(if s[19] == b'+' { b - off } else { b + off });
        }
        None
    }

    #[test]
    fn parse_ts_matches_reference() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z+ .ez\xd9";
        let templates: [&[u8]; 6] = [
            b"2027-12-19T18:13:05Z",
            b"2028-02-29T23:59:59Z",
            b"2027-02-28T00:00:00+05:30",
            b"1791071890000",
            b"0001791071890000",
            b"2026-10-04T01:30:00-00:00",
        ];
        // keep each case inside a big buffer so 16-byte reads stay in bounds
        let mut buf = vec![0u8; 8192];
        for i in 0..2_000_000u64 {
            let mut v: Vec<u8> = if i % 3 == 0 {
                let n = (rnd() % 26) as usize;
                (0..n).map(|_| alphabet[(rnd() % alphabet.len() as u64) as usize]).collect()
            } else {
                let mut t = templates[(rnd() % 6) as usize].to_vec();
                let k = (rnd() % 4) as usize;
                for _ in 0..k {
                    let pos = (rnd() as usize) % t.len();
                    t[pos] = alphabet[(rnd() % 10) as usize];
                }
                t
            };
            if i % 7 == 0 {
                v = (0..(rnd() % 24) as usize).map(|_| b'0' + (rnd() % 10) as u8).collect();
            }
            let off = 4000 + (rnd() % 200) as usize;
            buf[off..off + v.len()].copy_from_slice(&v);
            buf[off + v.len()..off + v.len() + 20].fill(b'7');
            let got = parse_ts(&buf[off..off + v.len()]);
            assert_eq!(got, ts_ref(&v), "input {:?}", String::from_utf8_lossy(&v));
        }
    }

    #[test]
    fn keys_roundtrip_and_order() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alpha: &[u8] = b"0123456789ABCXYZabcxyz _\"\r\t\xc3\xa9";
        let mut keys: Vec<(Vec<u8>, Key)> = Vec::new();
        for i in 0..200_000 {
            let n = (rnd() % 13) as usize;
            let lim = if i % 2 == 0 { 22 } else { alpha.len() };
            let v: Vec<u8> = (0..n).map(|_| alpha[(rnd() % lim as u64) as usize]).collect();
            let k = pack(&v);
            assert_eq!(unpack(k), v);
            keys.push((v, k));
        }
        for w in keys.windows(2) {
            let (a, ka) = &w[0];
            let (b, kb) = &w[1];
            assert_eq!(a.cmp(b), key_cmp(*ka, *kb), "{:?} {:?}", a, b);
            assert_eq!(a == b, ka == kb);
        }
    }

    #[test]
    fn scan_matches_scalar_rules() {
        let cases: [&[u8]; 9] = [
            b"a,b\n\"a\",\"\"\n\"a,b\n,\n\n\"\"\"\",x\na\"b,c\n\"a\",b",
            b"x,y",
            b"\n\n\n",
            b"\"q\",\"r\"\r\n",
            b"a,b,c\na,b\n",
            b"\"\",\"\"\n",
            b"\"a\"b,c\n",
            b"a,\"b\n",
            b"\",a\n",
        ];
        for c in cases {
            let mut got = Vec::new();
            scan::<2>(c, |f| got.push((f[0].to_vec(), f[1].to_vec())));
            let mut want = Vec::new();
            for line in c.split(|&b| b == b'\n') {
                if let Some(f) = split::<2>(line) {
                    want.push((f[0].to_vec(), f[1].to_vec()));
                }
            }
            assert_eq!(got, want, "case {:?}", String::from_utf8_lossy(c));
        }
    }
}
