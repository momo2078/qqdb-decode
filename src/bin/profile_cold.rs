use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context, Result};
use rayon::prelude::*;
use rusqlite::{params_from_iter, types::ValueRef, Connection, OpenFlags, ToSql};

#[path = "../elements.rs"]
mod elements;
#[path = "../raw.rs"]
mod raw;
#[path = "../references.rs"]
mod references;

#[derive(Debug)]
struct Options {
    db_path: PathBuf,
    index_db_path: Option<PathBuf>,
    refs_dir: Option<PathBuf>,
    batch_size: usize,
    update_limit: Option<usize>,
    table_like: Option<String>,
    conservative_write: bool,
}

#[derive(Debug, Clone)]
struct PreparedFastRow {
    rowid: i64,
    sender_uin: i64,
    raw_compact: String,
    reply_reference: Option<raw::ReplyReference>,
    sender_display: Option<String>,
    self_time_rand: Option<(i64, i64)>,
}

#[derive(Debug, Clone)]
struct LightCompactSourceRow {
    rowid: i64,
    sender_uin: i64,
    msg_content: Vec<u8>,
    sender_display: Option<String>,
    info: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ReplyLookupKey {
    target_seq: Option<u32>,
    target_time: Option<i64>,
    target_rand: Option<i64>,
}

impl From<&raw::ReplyReference> for ReplyLookupKey {
    fn from(reply: &raw::ReplyReference) -> Self {
        Self {
            target_seq: reply.target_seq,
            target_time: reply.target_time,
            target_rand: reply.target_rand,
        }
    }
}

impl ReplyLookupKey {
    fn from_parts(target_seq: Option<u32>, target_time: Option<i64>, target_rand: Option<i64>) -> Self {
        Self {
            target_seq,
            target_time,
            target_rand,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct StageStats {
    rows: usize,
    batches: usize,
    fetch: Duration,
    decode: Duration,
    reference: Duration,
    write: Duration,
    index_prefetch: Duration,
    reply_lookup: Duration,
}

const BULK_UPDATE_CHUNK_ROWS: usize = 4_096;

impl StageStats {
    fn add(&mut self, other: &StageStats) {
        self.rows += other.rows;
        self.batches += other.batches;
        self.fetch += other.fetch;
        self.decode += other.decode;
        self.reference += other.reference;
        self.write += other.write;
        self.index_prefetch += other.index_prefetch;
        self.reply_lookup += other.reply_lookup;
    }
}

fn main() -> Result<()> {
    let options = parse_args()?;
    let conn = Connection::open_with_flags(
        &options.db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("打开数据库失败: {:?}", options.db_path))?;
    raw::register_sqlite_functions(&conn)?;
    conn.set_prepared_statement_cache_capacity(512);
    if options.conservative_write {
        configure_conservative_update_pragmas(&conn)?;
    } else {
        configure_bulk_update_pragmas(&conn)?;
    }

    let reference_lookup = references::ReferenceLookup::open(
        &options.db_path,
        options.index_db_path.as_ref(),
        options.refs_dir.as_ref(),
    )?;

    let tables = list_message_tables(&conn, options.table_like.as_deref())?;
    if tables.is_empty() {
        bail!("没有找到包含 MsgContent 的表");
    }

    let total_start = Instant::now();
    let mut total = StageStats::default();
    let mut per_table = Vec::new();

    for table in tables {
        ensure_decoded_columns(&conn, &table)?;
        let stats = profile_table(
            &conn,
            &table,
            options.update_limit,
            options.batch_size,
            &reference_lookup,
        )?;
        total.add(&stats);
        per_table.push((table, stats));
    }
    let total_elapsed = total_start.elapsed();

    println!("=== cold profile summary ===");
    println!("db={}", options.db_path.display());
    println!("rows={}", total.rows);
    println!("batches={}", total.batches);
    println!("elapsed_total={}", fmt_duration(total_elapsed));
    println!("fetch={}", fmt_duration(total.fetch));
    println!("decode={}", fmt_duration(total.decode));
    println!("reference={}", fmt_duration(total.reference));
    println!("  index_prefetch={}", fmt_duration(total.index_prefetch));
    println!("  reply_lookup={}", fmt_duration(total.reply_lookup));
    println!("write={}", fmt_duration(total.write));
    println!();

    println!("=== per table ===");
    for (table, stats) in per_table {
        println!(
            "{} rows={} batches={} total={} fetch={} decode={} reference={} write={}",
            table,
            stats.rows,
            stats.batches,
            fmt_duration(stats.fetch + stats.decode + stats.reference + stats.write),
            fmt_duration(stats.fetch),
            fmt_duration(stats.decode),
            fmt_duration(stats.reference),
            fmt_duration(stats.write),
        );
    }

    Ok(())
}

fn parse_args() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let db_path = PathBuf::from(args.next().ok_or_else(|| {
        anyhow!(
            "用法: profile_cold <db_path> [--index-db PATH] [--refs-dir DIR] [--batch-size N] [--update-limit N] [--table-like SUBSTR] [--conservative]"
        )
    })?);

    let mut index_db_path = None;
    let mut refs_dir = None;
    let mut batch_size = 100_000usize;
    let mut update_limit = None;
    let mut table_like = None;
    let mut conservative_write = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--index-db" => {
                index_db_path = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--index-db 后缺少路径"))?,
                ));
            }
            "--refs-dir" => {
                refs_dir = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--refs-dir 后缺少路径"))?,
                ));
            }
            "--batch-size" => {
                batch_size = args
                    .next()
                    .ok_or_else(|| anyhow!("--batch-size 后缺少数字"))?
                    .parse::<usize>()
                    .context("无效的 --batch-size")?;
                if batch_size == 0 {
                    bail!("--batch-size 必须大于 0");
                }
            }
            "--update-limit" => {
                update_limit = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--update-limit 后缺少数字"))?
                        .parse::<usize>()
                        .context("无效的 --update-limit")?,
                );
            }
            "--table-like" => {
                table_like = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--table-like 后缺少字符串"))?,
                );
            }
            "--conservative" => conservative_write = true,
            other => bail!("未知参数: {other}"),
        }
    }

    Ok(Options {
        db_path,
        index_db_path,
        refs_dir,
        batch_size,
        update_limit,
        table_like,
        conservative_write,
    })
}

fn profile_table(
    conn: &Connection,
    table_name: &str,
    limit: Option<usize>,
    batch_size: usize,
    reference_lookup: &references::ReferenceLookup,
) -> Result<StageStats> {
    let quoted = quote_ident(table_name);
    let select_sql = format!(
        "SELECT rowid, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2"
    );
    let mut stats = StageStats::default();
    let mut last_rowid = 0_i64;
    let mut remaining = limit;
    let mut reply_lookup_cache = HashMap::<ReplyLookupKey, Option<i64>>::new();
    let mut seen_message_rowids = HashMap::<(i64, i64), i64>::new();

    loop {
        let current_batch_size = remaining
            .map(|value| value.min(batch_size))
            .unwrap_or(batch_size);
        if current_batch_size == 0 {
            break;
        }

        let start_fetch = Instant::now();
        let batch = fetch_fast_batch_compact_light(conn, &select_sql, last_rowid, current_batch_size)?;
        stats.fetch += start_fetch.elapsed();

        if batch.is_empty() {
            break;
        }

        stats.batches += 1;
        stats.rows += batch.len();
        last_rowid = batch.last().map(|row| row.rowid).unwrap_or(last_rowid);
        if let Some(remaining_rows) = remaining.as_mut() {
            *remaining_rows = remaining_rows.saturating_sub(batch.len());
        }

        let min_rowid = batch.first().map(|row| row.rowid).unwrap_or(last_rowid);
        let max_rowid = batch.last().map(|row| row.rowid).unwrap_or(last_rowid);

        let start_index = Instant::now();
        let index_texts = reference_lookup.prefetch_index_range(table_name, min_rowid, max_rowid)?;
        let elapsed_index = start_index.elapsed();
        stats.index_prefetch += elapsed_index;
        stats.reference += elapsed_index;

        let start_decode = Instant::now();
        let prepared_batch = batch
            .into_par_iter()
            .map(|row| {
                let LightCompactSourceRow {
                    rowid,
                    sender_uin,
                    msg_content,
                    sender_display,
                    info,
                } = row;
                let info_slice = info.as_deref().unwrap_or(&[]);
                match raw::decode_compact_with_context(&msg_content, info_slice) {
                    Ok(context) => PreparedFastRow {
                        rowid,
                        sender_uin,
                        raw_compact: context.text,
                        reply_reference: context.reply_reference,
                        sender_display: context.sender_show_name.or(sender_display),
                        self_time_rand: extract_self_time_rand(&msg_content),
                    },
                    Err(err) => PreparedFastRow {
                        rowid,
                        sender_uin,
                        raw_compact: format!("<decode error: {err}>"),
                        reply_reference: raw::extract_reply_reference(&msg_content),
                        sender_display: sender_display
                            .or_else(|| info.as_deref().and_then(raw::extract_sender_show_name)),
                        self_time_rand: extract_self_time_rand(&msg_content),
                    },
                }
            })
            .collect::<Vec<_>>();
        stats.decode += start_decode.elapsed();

        let start_reply = Instant::now();
        let current_batch_message_rowids = prepared_batch
            .iter()
            .filter_map(|row| row.self_time_rand.map(|time_rand| (time_rand, row.rowid)))
            .collect::<HashMap<_, _>>();
        let mut pending_reply_keys = Vec::with_capacity(prepared_batch.len());
        let mut pending_replies = Vec::new();
        let mut reply_target_rowids = prepared_batch
            .iter()
            .map(|row| {
                let reply_already_rendered = row.raw_compact.starts_with("[回复")
                    || index_texts
                        .get(&row.rowid)
                        .map(|text| text.starts_with("[回复"))
                        .unwrap_or(false);
                let mut pending_key = None;
                let resolved = (!reply_already_rendered)
                    .then_some(row.reply_reference.as_ref())
                    .flatten()
                    .and_then(|reply| {
                        if let (Some(time), Some(rand)) = (reply.target_time, reply.target_rand) {
                            if let Some(&rowid) = current_batch_message_rowids.get(&(time, rand)) {
                                return Some(rowid);
                            }
                            if let Some(&rowid) = seen_message_rowids.get(&(time, rand)) {
                                return Some(rowid);
                            }
                        }
                        let key = ReplyLookupKey::from(reply);
                        if let Some(cached) = reply_lookup_cache.get(&key) {
                            *cached
                        } else {
                            pending_key = Some(key);
                            pending_replies.push(reply);
                            None
                        }
                    });
                pending_reply_keys.push(pending_key);
                resolved
            })
            .collect::<Vec<_>>();
        if !pending_replies.is_empty() {
            if let Ok(prefetched) =
                reference_lookup.prefetch_reply_target_rowids_on(conn, table_name, &pending_replies)
            {
                for ((target_seq, target_time, target_rand), rowid) in prefetched {
                    reply_lookup_cache.insert(
                        ReplyLookupKey::from_parts(target_seq, target_time, target_rand),
                        rowid,
                    );
                }
            }
            for (resolved, pending_key) in reply_target_rowids.iter_mut().zip(pending_reply_keys.into_iter()) {
                if let Some(key) = pending_key {
                    *resolved = reply_lookup_cache.get(&key).copied().flatten();
                }
            }
        }
        seen_message_rowids.extend(current_batch_message_rowids);
        let elapsed_reply = start_reply.elapsed();
        stats.reply_lookup += elapsed_reply;
        stats.reference += elapsed_reply;

        let start_decode_assembly = Instant::now();
        let pubacc_names = reference_lookup.pubacc_names();
        let decoded_batch = prepared_batch
            .into_par_iter()
            .zip(reply_target_rowids.into_par_iter())
            .map(|(prepared, reply_target_rowid)| {
                let PreparedFastRow {
                    rowid,
                    sender_uin,
                    raw_compact,
                    reply_reference,
                    sender_display,
                    ..
                } = prepared;

                let readable = if let Some(index_text) = index_texts.get(&rowid) {
                    references::merge_preferred_text(index_text, &raw_compact)
                } else {
                    raw_compact
                };
                let readable = apply_reply_hint(readable, reply_reference.as_ref(), reply_target_rowid);
                let sender_display = sender_display.or_else(|| {
                    pubacc_names
                        .get(&sender_uin)
                        .map(|name| format!("{name}({sender_uin})"))
                });

                (rowid, readable, sender_display)
            })
            .collect::<Vec<_>>();
        stats.decode += start_decode_assembly.elapsed();

        let start_write = Instant::now();
        write_fast_compact_batch(conn, &quoted, &decoded_batch)?;
        stats.write += start_write.elapsed();
    }

    Ok(stats)
}

fn write_fast_compact_batch(
    conn: &Connection,
    quoted_table: &str,
    decoded_batch: &[(i64, String, Option<String>)],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let mut sql_cache = HashMap::<usize, String>::new();
    for chunk in decoded_batch.chunks(BULK_UPDATE_CHUNK_ROWS) {
        let sql = sql_cache
            .entry(chunk.len())
            .or_insert_with(|| build_bulk_update_compact_sql(quoted_table, chunk.len()));
        let mut params = Vec::<&dyn ToSql>::with_capacity(chunk.len() * 3);
        for (rowid, readable, sender_display) in chunk {
            params.push(rowid);
            params.push(readable);
            params.push(sender_display);
        }
        tx.prepare_cached(sql)?
            .execute(params_from_iter(params.into_iter()))?;
    }
    tx.commit()?;
    Ok(())
}

fn build_bulk_update_compact_sql(quoted_table: &str, row_count: usize) -> String {
    let values = std::iter::repeat("(?, ?, ?)")
        .take(row_count)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH data(rowid, decoded_msg, sender_display) AS (VALUES {values}) \
         UPDATE {quoted_table} \
         SET DecodedMsg = data.decoded_msg, SenderDisplay = data.sender_display \
         FROM data \
         WHERE {quoted_table}.rowid = data.rowid"
    )
}

fn extract_self_time_rand(msg_content: &[u8]) -> Option<(i64, i64)> {
    if msg_content.len() < 16 {
        return None;
    }
    let time = u32::from_le_bytes([
        *msg_content.get(8)?,
        *msg_content.get(9)?,
        *msg_content.get(10)?,
        *msg_content.get(11)?,
    ]) as i64;
    let rand = u32::from_le_bytes([
        *msg_content.get(12)?,
        *msg_content.get(13)?,
        *msg_content.get(14)?,
        *msg_content.get(15)?,
    ]) as i64;
    Some((time, rand))
}

fn fetch_fast_batch_compact(
    conn: &Connection,
    select_sql: &str,
    last_rowid: i64,
    current_batch_size: usize,
) -> Result<Vec<(i64, i64, Vec<u8>, Vec<u8>)>> {
    let mut select_stmt = conn.prepare_cached(select_sql)?;
    let mut rows = select_stmt.query((last_rowid, current_batch_size as i64))?;
    let mut batch = Vec::with_capacity(current_batch_size);
    while let Some(row) = rows.next()? {
        let rowid: i64 = row.get(0)?;
        let sender_uin: i64 = row.get(1)?;
        let msg_content: Vec<u8> = row.get(2)?;
        let info: Vec<u8> = row.get(3)?;
        batch.push((rowid, sender_uin, msg_content, info));
    }
    Ok(batch)
}

fn fetch_fast_batch_compact_light(
    conn: &Connection,
    select_sql: &str,
    last_rowid: i64,
    current_batch_size: usize,
) -> Result<Vec<LightCompactSourceRow>> {
    let mut select_stmt = conn.prepare_cached(select_sql)?;
    let mut rows = select_stmt.query((last_rowid, current_batch_size as i64))?;
    let mut batch = Vec::with_capacity(current_batch_size);
    while let Some(row) = rows.next()? {
        let rowid: i64 = row.get(0)?;
        let sender_uin: i64 = row.get(1)?;
        let msg_content: Vec<u8> = row.get(2)?;
        let info_ref = row.get_ref(3)?;
        let info_bytes = match info_ref {
            ValueRef::Blob(bytes) => bytes,
            ValueRef::Null => &[],
            _ => &[],
        };
        let sender_display = raw::extract_sender_show_name(info_bytes);
        let info = if raw::compact_decode_requires_full_info(&msg_content) && !info_bytes.is_empty() {
            Some(info_bytes.to_vec())
        } else {
            None
        };
        batch.push(LightCompactSourceRow {
            rowid,
            sender_uin,
            msg_content,
            sender_display,
            info,
        });
    }
    Ok(batch)
}

fn list_message_tables(conn: &Connection, table_like: Option<&str>) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")?;
    let names = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut tables = Vec::new();

    for name in names {
        let name = name?;
        if name.contains('$') {
            continue;
        }
        if let Some(pattern) = table_like {
            if !name.contains(pattern) {
                continue;
            }
        }
        if table_has_column(conn, &name, "MsgContent")? {
            tables.push(name);
        }
    }

    Ok(tables)
}

fn ensure_decoded_columns(conn: &Connection, table_name: &str) -> Result<()> {
    ensure_text_column(conn, table_name, "DecodedMsg")?;
    ensure_text_column(conn, table_name, "SenderDisplay")?;
    Ok(())
}

fn ensure_text_column(conn: &Connection, table_name: &str, column_name: &str) -> Result<()> {
    if table_has_column(conn, table_name, column_name)? {
        return Ok(());
    }
    let sql = format!(
        "ALTER TABLE {} ADD COLUMN {} TEXT",
        quote_ident(table_name),
        quote_ident(column_name),
    );
    conn.execute(&sql, [])?;
    Ok(())
}

fn table_has_column(conn: &Connection, table_name: &str, column_name: &str) -> Result<bool> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table_name));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == column_name {
            return Ok(true);
        }
    }
    Ok(false)
}

fn configure_bulk_update_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA locking_mode = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -262144;
PRAGMA mmap_size = 4294967296;
        PRAGMA cache_spill = OFF;
        PRAGMA foreign_keys = OFF;
        ",
    )?;
    Ok(())
}

fn configure_conservative_update_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = FULL;
        PRAGMA locking_mode = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -131072;
PRAGMA mmap_size = 4294967296;
        ",
    )?;
    Ok(())
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn apply_reply_hint(
    text: String,
    reply_reference: Option<&raw::ReplyReference>,
    reply_target_rowid: Option<i64>,
) -> String {
    let Some(reply_reference) = reply_reference else {
        return text;
    };
    if text.starts_with("[回复") {
        return text;
    }

    let prefix = if let Some(target_rowid) = reply_target_rowid {
        format!("[回复 rowid={target_rowid}]")
    } else if let Some(seq) = reply_reference.target_seq {
        format!("[回复 seq={seq}]")
    } else {
        "[回复]".to_string()
    };

    if text.is_empty() {
        prefix
    } else {
        format!("{prefix} {text}")
    }
}

fn fmt_duration(duration: Duration) -> String {
    format!("{:.3}s", duration.as_secs_f64())
}
