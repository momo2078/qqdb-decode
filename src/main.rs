use std::{collections::HashMap, path::PathBuf, thread::JoinHandle};
use anyhow::{anyhow, bail, Context, Result};
use rayon::prelude::*;
use rusqlite::{params_from_iter, types::ValueRef, Connection, OpenFlags, OptionalExtension, ToSql};
use serde::Serialize;
use tracing::{event, Level};

mod elements;
mod raw;
mod references;

#[derive(Debug, Clone)]
struct Options {
    db_path: PathBuf,
    index_db_path: Option<PathBuf>,
    refs_dir: Option<PathBuf>,
    preview_limit: usize,
    update_db: bool,
    with_detail_json: bool,
    conservative_write: bool,
    batch_size: usize,
    update_limit: Option<usize>,
    table_like: Option<String>,
}

#[derive(Debug)]
struct DecodedWriteback {
    readable: String,
    sender_display: Option<String>,
    detail_json: Option<String>,
}

#[derive(Debug)]
struct PreparedFastRow {
    rowid: i64,
    sender_uin: i64,
    raw_compact: String,
    reply_reference: Option<raw::ReplyReference>,
    sender_display: Option<String>,
    self_time_rand: Option<(i64, i64)>,
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

#[derive(Debug)]
struct FastRangeFailure {
    error: anyhow::Error,
    processed_rows: usize,
    fail_start: i64,
    fail_end: i64,
}

#[derive(Debug)]
struct RewritePlan {
    temp_table_name: String,
    create_sql: String,
    select_sql: String,
    insert_sql: String,
    preserve_source_detail_json: bool,
}

#[derive(Debug)]
struct RewriteSourceRow {
    rowid: i64,
    time: i64,
    rand: i64,
    sender_uin: i64,
    msg_content: Vec<u8>,
    info: Vec<u8>,
    preserved_detail_json: Option<String>,
}

type CompactSourceRow = (i64, i64, Vec<u8>, Vec<u8>);
type CompactDecodedRow = (i64, DecodedWriteback);

const BULK_UPDATE_CHUNK_ROWS: usize = 4_096;

#[derive(Debug)]
struct LightCompactSourceRow {
    rowid: i64,
    sender_uin: i64,
    msg_content: Vec<u8>,
    sender_display: Option<String>,
    info: Option<Vec<u8>>,
}

#[derive(Debug)]
struct PreparedCompactBatch {
    min_rowid: i64,
    max_rowid: i64,
    rows: Vec<PreparedFastRow>,
}

#[derive(Debug)]
struct PrefetchedCompactBatch {
    lower_exclusive: i64,
    requested_batch_size: usize,
    result: std::result::Result<Vec<LightCompactSourceRow>, String>,
}

#[derive(Debug)]
struct PrefetchedCompactBatchHandle {
    lower_exclusive: i64,
    requested_batch_size: usize,
    handle: JoinHandle<std::result::Result<Vec<LightCompactSourceRow>, String>>,
}

#[derive(Debug, Serialize)]
struct DecodedDetailRecord {
    table: String,
    rowid: i64,
    time: i64,
    rand: i64,
    sender_uin: i64,
    sender_display: Option<String>,
    decoded_readable: String,
    decoded_raw: String,
    index_text: Option<String>,
    fold_prompt: Option<String>,
    pubacc_name: Option<String>,
    reply_reference: Option<raw::ReplyReference>,
    reply_target_rowid: Option<i64>,
    trace: Option<raw::MessageTrace>,
    trace_error: Option<String>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .init();

    let options = parse_args()?;
    event!(Level::INFO, "正在处理 {:?} 数据库", options.db_path);

    if !options.db_path.exists() {
        bail!("数据库文件 {:?} 不存在", options.db_path);
    }
    if !options.db_path.is_file() {
        bail!("{:?} 不是一个文件", options.db_path);
    }

    let conn = Connection::open_with_flags(
        &options.db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("打开数据库失败: {:?}", options.db_path))?;
    raw::register_sqlite_functions(&conn)?;
    conn.set_prepared_statement_cache_capacity(512);
    if options.update_db {
        if options.conservative_write {
            configure_conservative_update_pragmas(&conn)?;
        } else {
            configure_bulk_update_pragmas(&conn)?;
        }
    } else {
        configure_read_pragmas(&conn)?;
    }
    let reference_lookup = references::ReferenceLookup::open(
        &options.db_path,
        options.index_db_path.as_ref(),
        options.refs_dir.as_ref(),
    )?;
    for source in reference_lookup.loaded_sources() {
        event!(Level::INFO, "已加载{}", source);
    }
    let tables = list_message_tables(&conn, options.table_like.as_deref())?;

    if tables.is_empty() {
        event!(Level::WARN, "没有找到包含 MsgContent 的表");
        return Ok(());
    }

    if options.update_db {
        for table in &tables {
            update_table(
                &conn,
                table,
                options.update_limit,
                options.with_detail_json,
                options.conservative_write,
                options.batch_size,
                &reference_lookup,
            )?;
        }
    }

    for table in &tables {
        preview_table(&conn, table, options.preview_limit, &reference_lookup)?;
    }

    Ok(())
}

fn parse_args() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let db_path = PathBuf::from(args.next().ok_or_else(|| {
        anyhow!(
            "缺少数据库路径，用法: qqdb-decode <db_path> [--index-db PATH] [--refs-dir DIR] [--preview-limit N] [--update-db] [--with-detail-json] [--conservative] [--batch-size N] [--update-limit N] [--table-like SUBSTR]"
        )
    })?);

    let mut index_db_path = None;
    let mut refs_dir = None;
    let mut preview_limit = 3_usize;
    let mut update_db = false;
    let mut with_detail_json = false;
    let mut conservative_write = false;
    let mut batch_size = 200_000_usize;
    let mut update_limit = None;
    let mut table_like = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--index-db" => {
                index_db_path = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--index-db 后面缺少数据库路径"))?,
                ));
            }
            "--refs-dir" => {
                refs_dir = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--refs-dir 后面缺少目录路径"))?,
                ));
            }
            "--preview-limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--preview-limit 后面缺少数字"))?;
                preview_limit = value
                    .parse::<usize>()
                    .with_context(|| format!("无效的 --preview-limit: {value}"))?;
            }
            "--update-db" => update_db = true,
            "--with-detail-json" => with_detail_json = true,
            "--conservative" | "--safe-write" => conservative_write = true,
            "--batch-size" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--batch-size 后面缺少数字"))?;
                batch_size = value
                    .parse::<usize>()
                    .with_context(|| format!("无效的 --batch-size: {value}"))?;
                if batch_size == 0 {
                    bail!("--batch-size 必须大于 0");
                }
            }
            "--update-limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--update-limit 后面缺少数字"))?;
                update_limit = Some(
                    value
                        .parse::<usize>()
                        .with_context(|| format!("无效的 --update-limit: {value}"))?,
                );
            }
            "--table-like" => {
                table_like = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--table-like 后面缺少匹配字符串"))?,
                );
            }
            other => bail!("未知参数: {other}"),
        }
    }

    Ok(Options {
        db_path,
        index_db_path,
        refs_dir,
        preview_limit,
        update_db,
        with_detail_json,
        conservative_write,
        batch_size,
        update_limit,
        table_like,
    })
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

fn preview_table(
    conn: &Connection,
    table_name: &str,
    limit: usize,
    reference_lookup: &references::ReferenceLookup,
) -> Result<()> {
    if limit == 0 {
        return Ok(());
    }

    let quoted = quote_ident(table_name);
    let sql =
        format!("SELECT rowid, Time, Rand, SenderUin, MsgContent, Info FROM {quoted} LIMIT ?1");
    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            event!(Level::WARN, "预览表 {} 失败，已跳过: {}", table_name, err);
            return Ok(());
        }
    };
    let rows = match stmt.query_map([limit as i64], |row| {
        let rowid: i64 = row.get(0)?;
        let time: i64 = row.get(1)?;
        let rand: i64 = row.get(2)?;
        let sender_uin: i64 = row.get(3)?;
        let msg_content: Vec<u8> = row.get(4)?;
        let info: Vec<u8> = row.get(5)?;
        Ok((rowid, time, rand, sender_uin, msg_content, info))
    }) {
        Ok(rows) => rows,
        Err(err) => {
            event!(Level::WARN, "预览表 {} 失败，已跳过: {}", table_name, err);
            return Ok(());
        }
    };

    event!(Level::INFO, "预览表 {}", table_name);
    for row in rows {
        let (rowid, time, rand, sender_uin, msg_content, info) = match row {
            Ok(row) => row,
            Err(err) => {
                event!(Level::WARN, "预览表 {} 时跳过损坏行: {}", table_name, err);
                continue;
            }
        };
        let decoded = decode_for_writeback(
            conn,
            table_name,
            rowid,
            time,
            rand,
            sender_uin,
            &msg_content,
            &info,
            false,
            reference_lookup,
        );
        let sender = decoded
            .sender_display
            .clone()
            .unwrap_or_else(|| sender_uin.to_string());
        println!(
            "[{table_name}] rowid={rowid} sender={sender} => {}",
            decoded.readable
        );
    }
    Ok(())
}

fn update_table(
    conn: &Connection,
    table_name: &str,
    limit: Option<usize>,
    with_detail_json: bool,
    _conservative_write: bool,
    batch_size: usize,
    reference_lookup: &references::ReferenceLookup,
) -> Result<()> {
    event!(Level::INFO, "写回表 {}", table_name);

    if let Err(err) = ensure_decoded_columns(conn, table_name, with_detail_json) {
        event!(
            Level::WARN,
            "表 {} 无法加列/检查列，整表跳过: {}",
            table_name,
            err
        );
        return Ok(());
    }

    match update_table_fast(
        conn,
        table_name,
        limit,
        with_detail_json,
        batch_size,
        reference_lookup,
    ) {
        Ok(processed) => {
            event!(
                Level::INFO,
                "表 {} 写回完成，共 {} 条",
                table_name,
                processed
            );
            Ok(())
        }
        Err(failure) => {
            event!(
                Level::WARN,
                "表 {} 快速写回遇到损坏，切换到跳坏行模式: {}",
                table_name,
                failure.error
            );
            let remaining_limit = limit.map(|value| value.saturating_sub(failure.processed_rows));
            match update_table_resilient_from_failure(
                conn,
                table_name,
                &failure,
                remaining_limit,
                with_detail_json,
                batch_size,
                reference_lookup,
            ) {
                Ok((processed, skipped)) => {
                    event!(
                        Level::INFO,
                        "表 {} 写回完成，共 {} 条，跳过损坏行 {} 条",
                        table_name,
                        processed,
                        skipped
                    );
                }
                Err(resilient_err) => {
                    event!(
                        Level::WARN,
                        "表 {} 跳坏行模式仍失败，整表剩余内容跳过: {}",
                        table_name,
                        resilient_err
                    );
                }
            }
            Ok(())
        }
    }
}

fn rewrite_table_fast(
    conn: &Connection,
    table_name: &str,
    with_detail_json: bool,
    batch_size: usize,
    reference_lookup: &references::ReferenceLookup,
) -> Result<usize> {
    let Some(plan) = build_rewrite_plan(conn, table_name, with_detail_json)? else {
        bail!("当前表结构不适合重建式写回");
    };

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(&plan.create_sql)?;

    let mut last_rowid = 0_i64;
    let mut processed = 0_usize;
    let mut next_report = 1000_usize;

    loop {
        let batch = fetch_rewrite_batch(
            &tx,
            &plan.select_sql,
            last_rowid,
            batch_size,
            plan.preserve_source_detail_json,
        )?;
        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len();
        last_rowid = batch.last().map(|row| row.rowid).unwrap_or(last_rowid);
        let min_rowid = batch.first().map(|row| row.rowid).unwrap_or(last_rowid);
        let max_rowid = batch.last().map(|row| row.rowid).unwrap_or(last_rowid);
        let index_texts = reference_lookup.prefetch_index_range(table_name, min_rowid, max_rowid)?;
        let pubacc_names = reference_lookup.pubacc_names();

        let mut insert_stmt = tx.prepare_cached(&plan.insert_sql)?;

        if with_detail_json {
            for RewriteSourceRow {
                rowid,
                time,
                rand,
                sender_uin,
                msg_content,
                info,
                ..
            } in batch
            {
                let decoded = decode_for_writeback(
                    &tx,
                    table_name,
                    rowid,
                    time,
                    rand,
                    sender_uin,
                    &msg_content,
                    &info,
                    with_detail_json,
                    reference_lookup,
                );
                insert_stmt.execute((
                    rowid,
                    time,
                    rand,
                    sender_uin,
                    msg_content,
                    info,
                    decoded.readable,
                    decoded.sender_display,
                    decoded.detail_json,
                ))?;
            }
        } else {
            let prepared_batch = batch
                .into_par_iter()
                .map(|row| {
                    let RewriteSourceRow {
                        rowid,
                        time,
                        rand,
                        sender_uin,
                        msg_content,
                        info,
                        preserved_detail_json,
                    } = row;
                    let prepared = match raw::decode_compact_with_context(&msg_content, &info) {
                        Ok(context) => PreparedFastRow {
                            rowid,
                            sender_uin,
                            raw_compact: context.text,
                            reply_reference: context.reply_reference,
                            sender_display: context.sender_show_name,
                            self_time_rand: extract_self_time_rand(&msg_content),
                        },
                        Err(err) => PreparedFastRow {
                            rowid,
                            sender_uin,
                            raw_compact: format!("<decode error: {err}>"),
                            reply_reference: raw::extract_reply_reference(&msg_content),
                            sender_display: raw::extract_sender_show_name(&info),
                            self_time_rand: extract_self_time_rand(&msg_content),
                        },
                    };
                    (
                        RewriteSourceRow {
                            rowid,
                            time,
                            rand,
                            sender_uin,
                            msg_content,
                            info,
                            preserved_detail_json,
                        },
                        prepared,
                    )
                })
                .collect::<Vec<_>>();
            let reply_target_rowids = prepared_batch
                .iter()
                .scan(HashMap::<ReplyLookupKey, Option<i64>>::new(), |cache, (_, prepared)| {
                    let reply_already_rendered = prepared.raw_compact.starts_with("[回复")
                        || index_texts
                            .get(&prepared.rowid)
                            .map(|text| text.starts_with("[回复"))
                            .unwrap_or(false);
                    let resolved = (!reply_already_rendered)
                        .then_some(prepared.reply_reference.as_ref())
                        .flatten()
                        .and_then(|reply| {
                            let key = ReplyLookupKey::from(reply);
                            if let Some(cached) = cache.get(&key) {
                                *cached
                            } else {
                                let rowid = reference_lookup
                                    .reply_target_rowid_on(&tx, table_name, reply)
                                    .ok()
                                    .flatten();
                                cache.insert(key, rowid);
                                rowid
                            }
                        });
                    Some(resolved)
                })
                .collect::<Vec<_>>();

            for (
                (
                    RewriteSourceRow {
                        time,
                        rand,
                        sender_uin,
                        msg_content,
                        info,
                        preserved_detail_json,
                        ..
                    },
                    PreparedFastRow {
                        rowid,
                        sender_uin: prepared_sender_uin,
                        raw_compact,
                        reply_reference,
                        sender_display,
                        ..
                    },
                ),
                reply_target_rowid,
            ) in prepared_batch.into_iter().zip(reply_target_rowids.into_iter())
            {
                let readable = if let Some(index_text) = index_texts.get(&rowid) {
                    references::merge_preferred_text(index_text, &raw_compact)
                } else {
                    raw_compact
                };
                let readable =
                    apply_reply_hint(readable, reply_reference.as_ref(), reply_target_rowid);
                let sender_display = sender_display.or_else(|| {
                    pubacc_names
                        .get(&prepared_sender_uin)
                        .map(|name| format!("{name}({prepared_sender_uin})"))
                });

                if plan.preserve_source_detail_json {
                    insert_stmt.execute((
                        rowid,
                        time,
                        rand,
                        sender_uin,
                        msg_content,
                        info,
                        readable,
                        sender_display,
                        preserved_detail_json,
                    ))?;
                } else {
                    insert_stmt.execute((
                        rowid,
                        time,
                        rand,
                        sender_uin,
                        msg_content,
                        info,
                        readable,
                        sender_display,
                    ))?;
                }
            }
        }

        processed += batch_len;
        if processed >= next_report {
            event!(Level::INFO, "表 {} 已重建写回 {} 条", table_name, processed);
            next_report = ((processed / 1_000) + 1) * 1_000;
        }
    }

    tx.execute(&format!("DROP TABLE {}", quote_ident(table_name)), [])?;
    tx.execute(
        &format!(
            "ALTER TABLE {} RENAME TO {}",
            quote_ident(&plan.temp_table_name),
            quote_ident(table_name)
        ),
        [],
    )?;
    tx.commit()?;
    Ok(processed)
}

fn build_rewrite_plan(
    conn: &Connection,
    table_name: &str,
    with_detail_json: bool,
) -> Result<Option<RewritePlan>> {
    let quoted = quote_ident(table_name);
    let create_sql = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?1 LIMIT 1",
            [table_name],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let Some(create_sql) = create_sql else {
        return Ok(None);
    };
    if create_sql.to_ascii_lowercase().contains("without rowid") {
        return Ok(None);
    }

    let mut stmt = conn.prepare(&format!("PRAGMA table_info({quoted})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }

    const ALLOWED_COLUMNS: &[&str] = &[
        "Time",
        "Rand",
        "SenderUin",
        "MsgContent",
        "Info",
        "DecodedMsg",
        "SenderDisplay",
        "DecodedDetailJson",
    ];
    if columns
        .iter()
        .any(|column| !ALLOWED_COLUMNS.contains(&column.as_str()))
    {
        return Ok(None);
    }
    for required in ["Time", "Rand", "SenderUin", "MsgContent", "Info"] {
        if !columns.iter().any(|column| column == required) {
            return Ok(None);
        }
    }

    let has_decoded_msg = columns.iter().any(|column| column == "DecodedMsg");
    let has_sender_display = columns.iter().any(|column| column == "SenderDisplay");
    let has_detail_json = columns.iter().any(|column| column == "DecodedDetailJson");
    let preserve_source_detail_json = has_detail_json && !with_detail_json;

    let temp_table_name = format!("__qqdb_decode_rewrite__{table_name}");
    let create_sql = rewrite_create_table_sql(
        &create_sql,
        &temp_table_name,
        !has_decoded_msg,
        !has_sender_display,
        with_detail_json && !has_detail_json,
    )?;
    let create_sql = format!(
        "DROP TABLE IF EXISTS {temp};\n{create_sql}",
        temp = quote_ident(&temp_table_name)
    );

    let select_sql = if preserve_source_detail_json {
        format!(
            "SELECT rowid, Time, Rand, SenderUin, MsgContent, Info, DecodedDetailJson FROM {quoted} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2"
        )
    } else {
        format!(
            "SELECT rowid, Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2"
        )
    };

    let quoted_temp = quote_ident(&temp_table_name);
    let insert_sql = if with_detail_json || preserve_source_detail_json {
        format!(
            "INSERT INTO {quoted_temp} (rowid, Time, Rand, SenderUin, MsgContent, Info, DecodedMsg, SenderDisplay, DecodedDetailJson) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
        )
    } else {
        format!(
            "INSERT INTO {quoted_temp} (rowid, Time, Rand, SenderUin, MsgContent, Info, DecodedMsg, SenderDisplay) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
        )
    };

    Ok(Some(RewritePlan {
        temp_table_name,
        create_sql,
        select_sql,
        insert_sql,
        preserve_source_detail_json,
    }))
}

fn rewrite_create_table_sql(
    original_sql: &str,
    new_table_name: &str,
    add_decoded_msg: bool,
    add_sender_display: bool,
    add_detail_json: bool,
) -> Result<String> {
    let open = original_sql
        .find('(')
        .ok_or_else(|| anyhow!("建表 SQL 缺少 '('"))?;
    let close = original_sql
        .rfind(')')
        .ok_or_else(|| anyhow!("建表 SQL 缺少 ')'"))?;
    if close <= open {
        bail!("建表 SQL 结构异常");
    }

    let mut extra = String::new();
    if add_decoded_msg {
        extra.push_str(", \"DecodedMsg\" TEXT");
    }
    if add_sender_display {
        extra.push_str(", \"SenderDisplay\" TEXT");
    }
    if add_detail_json {
        extra.push_str(", \"DecodedDetailJson\" TEXT");
    }

    Ok(format!(
        "CREATE TABLE {}{}{}{}",
        quote_ident(new_table_name),
        &original_sql[open..close],
        extra,
        &original_sql[close..]
    ))
}

fn fetch_fast_batch(
    conn: &Connection,
    select_sql: &str,
    last_rowid: i64,
    upper_inclusive: Option<i64>,
    current_batch_size: usize,
) -> Result<Vec<(i64, i64, i64, i64, Vec<u8>, Vec<u8>)>> {
    let mut select_stmt = conn.prepare_cached(select_sql)?;
    let mut rows = if let Some(upper) = upper_inclusive {
        select_stmt.query((last_rowid, upper, current_batch_size as i64))?
    } else {
        select_stmt.query((last_rowid, current_batch_size as i64))?
    };
    let mut batch = Vec::with_capacity(current_batch_size);
    while let Some(row) = rows.next()? {
        let rowid: i64 = row.get(0)?;
        let time: i64 = row.get(1)?;
        let rand: i64 = row.get(2)?;
        let sender_uin: i64 = row.get(3)?;
        let msg_content: Vec<u8> = row.get(4)?;
        let info: Vec<u8> = row.get(5)?;
        batch.push((rowid, time, rand, sender_uin, msg_content, info));
    }
    Ok(batch)
}

fn fetch_fast_batch_compact(
    conn: &Connection,
    select_sql: &str,
    last_rowid: i64,
    upper_inclusive: Option<i64>,
    current_batch_size: usize,
) -> Result<Vec<(i64, i64, Vec<u8>, Vec<u8>)>> {
    let mut select_stmt = conn.prepare_cached(select_sql)?;
    let mut rows = if let Some(upper) = upper_inclusive {
        select_stmt.query((last_rowid, upper, current_batch_size as i64))?
    } else {
        select_stmt.query((last_rowid, current_batch_size as i64))?
    };
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
    upper_inclusive: Option<i64>,
    current_batch_size: usize,
) -> Result<Vec<LightCompactSourceRow>> {
    let mut select_stmt = conn.prepare_cached(select_sql)?;
    let mut rows = if let Some(upper) = upper_inclusive {
        select_stmt.query((last_rowid, upper, current_batch_size as i64))?
    } else {
        select_stmt.query((last_rowid, current_batch_size as i64))?
    };
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
        let needs_full_info = raw::compact_decode_requires_full_info(&msg_content);
        let sender_display = if needs_full_info {
            None
        } else {
            raw::extract_sender_show_name(info_bytes)
        };
        let info = if needs_full_info && !info_bytes.is_empty() {
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

fn prepare_fast_compact_rows(batch: Vec<CompactSourceRow>) -> PreparedCompactBatch {
    let min_rowid = batch.first().map(|row| row.0).unwrap_or_default();
    let max_rowid = batch.last().map(|row| row.0).unwrap_or_default();
    let rows = batch
        .into_par_iter()
        .map(
            |(rowid, sender_uin, msg_content, info)| match raw::decode_compact_with_context(&msg_content, &info) {
                Ok(context) => PreparedFastRow {
                    rowid,
                    sender_uin,
                    raw_compact: context.text,
                    reply_reference: context.reply_reference,
                    sender_display: context.sender_show_name,
                    self_time_rand: extract_self_time_rand(&msg_content),
                },
                Err(err) => PreparedFastRow {
                    rowid,
                    sender_uin,
                    raw_compact: format!("<decode error: {err}>"),
                    reply_reference: raw::extract_reply_reference(&msg_content),
                    sender_display: raw::extract_sender_show_name(&info),
                    self_time_rand: extract_self_time_rand(&msg_content),
                },
            },
        )
        .collect::<Vec<_>>();
    PreparedCompactBatch {
        min_rowid,
        max_rowid,
        rows,
    }
}

fn prepare_fast_compact_rows_light(batch: Vec<LightCompactSourceRow>) -> PreparedCompactBatch {
    let min_rowid = batch.first().map(|row| row.rowid).unwrap_or_default();
    let max_rowid = batch.last().map(|row| row.rowid).unwrap_or_default();
    let rows = batch
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
                    sender_display: sender_display.or_else(|| info.as_deref().and_then(raw::extract_sender_show_name)),
                    self_time_rand: extract_self_time_rand(&msg_content),
                },
            }
        })
        .collect::<Vec<_>>();
    PreparedCompactBatch {
        min_rowid,
        max_rowid,
        rows,
    }
}

fn spawn_prefetch_compact_batch(
    db_path: PathBuf,
    select_sql: String,
    last_rowid: i64,
    current_batch_size: usize,
) -> PrefetchedCompactBatchHandle {
    let handle = std::thread::spawn(move || {
        (|| -> Result<Vec<LightCompactSourceRow>> {
            let conn = Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .with_context(|| format!("打开预取数据库失败: {:?}", db_path))?;
            conn.set_prepared_statement_cache_capacity(64);
            configure_read_pragmas(&conn)?;
            fetch_fast_batch_compact_light(&conn, &select_sql, last_rowid, None, current_batch_size)
        })()
        .map_err(|err| err.to_string())
    });

    PrefetchedCompactBatchHandle {
        lower_exclusive: last_rowid,
        requested_batch_size: current_batch_size,
        handle,
    }
}

fn join_prefetched_compact_batch(handle: PrefetchedCompactBatchHandle) -> PrefetchedCompactBatch {
    let PrefetchedCompactBatchHandle {
        lower_exclusive,
        requested_batch_size,
        handle,
    } = handle;
    let result = handle
        .join()
        .unwrap_or_else(|_| Err("后台预取线程发生 panic".to_string()));
    PrefetchedCompactBatch {
        lower_exclusive,
        requested_batch_size,
        result,
    }
}

fn prepare_fast_compact_batch(
    conn: &Connection,
    table_name: &str,
    batch: Vec<CompactSourceRow>,
    reference_lookup: &references::ReferenceLookup,
    reply_lookup_cache: &mut HashMap<ReplyLookupKey, Option<i64>>,
    seen_message_rowids: &mut HashMap<(i64, i64), i64>,
) -> Result<(Vec<CompactDecodedRow>, i64)> {
    let prepared_batch = prepare_fast_compact_rows(batch);
    finalize_prepared_fast_compact_batch(
        conn,
        table_name,
        prepared_batch,
        reference_lookup,
        reply_lookup_cache,
        seen_message_rowids,
    )
}

fn prepare_fast_compact_batch_light(
    conn: &Connection,
    table_name: &str,
    batch: Vec<LightCompactSourceRow>,
    reference_lookup: &references::ReferenceLookup,
    reply_lookup_cache: &mut HashMap<ReplyLookupKey, Option<i64>>,
    seen_message_rowids: &mut HashMap<(i64, i64), i64>,
) -> Result<(Vec<CompactDecodedRow>, i64)> {
    let prepared_batch = prepare_fast_compact_rows_light(batch);
    finalize_prepared_fast_compact_batch(
        conn,
        table_name,
        prepared_batch,
        reference_lookup,
        reply_lookup_cache,
        seen_message_rowids,
    )
}

fn finalize_prepared_fast_compact_batch(
    conn: &Connection,
    table_name: &str,
    prepared_batch: PreparedCompactBatch,
    reference_lookup: &references::ReferenceLookup,
    reply_lookup_cache: &mut HashMap<ReplyLookupKey, Option<i64>>,
    seen_message_rowids: &mut HashMap<(i64, i64), i64>,
) -> Result<(Vec<CompactDecodedRow>, i64)> {
    let PreparedCompactBatch {
        min_rowid,
        max_rowid,
        rows: prepared_batch,
    } = prepared_batch;
    let index_texts = reference_lookup.prefetch_index_range(table_name, min_rowid, max_rowid)?;
    let pubacc_names = reference_lookup.pubacc_names();
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
        for (resolved, pending_key) in reply_target_rowids
            .iter_mut()
            .zip(pending_reply_keys.into_iter())
        {
            if let Some(key) = pending_key {
                *resolved = reply_lookup_cache.get(&key).copied().flatten();
            }
        }
    }
    seen_message_rowids.extend(current_batch_message_rowids);

    let decoded_batch = prepared_batch
        .into_par_iter()
        .zip(reply_target_rowids.into_par_iter())
        .map(|row| {
            let (
                PreparedFastRow {
                    rowid,
                    sender_uin,
                    raw_compact,
                    reply_reference,
                    sender_display,
                    ..
                },
                reply_target_rowid,
            ) = row;

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

            (
                rowid,
                DecodedWriteback {
                    readable,
                    sender_display,
                    detail_json: None,
                },
            )
        })
        .collect::<Vec<_>>();
    Ok((decoded_batch, max_rowid))
}

fn write_fast_compact_batch(
    conn: &Connection,
    quoted_table: &str,
    decoded_batch: &[CompactDecodedRow],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let mut sql_cache = HashMap::<usize, String>::new();
    for chunk in decoded_batch.chunks(BULK_UPDATE_CHUNK_ROWS) {
        let sql = sql_cache
            .entry(chunk.len())
            .or_insert_with(|| build_bulk_update_compact_sql(quoted_table, chunk.len()));
        let mut params = Vec::<&dyn ToSql>::with_capacity(chunk.len() * 3);
        for (rowid, decoded) in chunk {
            params.push(rowid);
            params.push(&decoded.readable);
            params.push(&decoded.sender_display);
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

fn fetch_rewrite_batch(
    conn: &Connection,
    select_sql: &str,
    last_rowid: i64,
    current_batch_size: usize,
    preserve_source_detail_json: bool,
) -> Result<Vec<RewriteSourceRow>> {
    let mut select_stmt = conn.prepare_cached(select_sql)?;
    let mut rows = select_stmt.query((last_rowid, current_batch_size as i64))?;
    let mut batch = Vec::with_capacity(current_batch_size);
    while let Some(row) = rows.next()? {
        batch.push(RewriteSourceRow {
            rowid: row.get(0)?,
            time: row.get(1)?,
            rand: row.get(2)?,
            sender_uin: row.get(3)?,
            msg_content: row.get(4)?,
            info: row.get(5)?,
            preserved_detail_json: if preserve_source_detail_json {
                row.get(6)?
            } else {
                None
            },
        });
    }
    Ok(batch)
}

fn update_table_fast(
    conn: &Connection,
    table_name: &str,
    limit: Option<usize>,
    with_detail_json: bool,
    batch_size: usize,
    reference_lookup: &references::ReferenceLookup,
) -> std::result::Result<usize, FastRangeFailure> {
    update_table_fast_range(
        conn,
        table_name,
        None,
        None,
        limit,
        with_detail_json,
        batch_size,
        None,
        reference_lookup,
        0,
    )
}

#[allow(clippy::too_many_arguments)]
fn update_table_fast_range_compact_pipelined(
    conn: &Connection,
    db_path: PathBuf,
    table_name: &str,
    lower_exclusive: Option<i64>,
    limit: Option<usize>,
    batch_size: usize,
    initial_batch_size: Option<usize>,
    reference_lookup: &references::ReferenceLookup,
    initial_processed: usize,
) -> std::result::Result<usize, FastRangeFailure> {
    let quoted = quote_ident(table_name);
    let select_sql = format!(
        "SELECT rowid, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2"
    );

    let mut processed = initial_processed;
    let mut last_rowid = lower_exclusive.unwrap_or(0_i64);
    let mut remaining = limit;
    let mut next_report = ((processed / 1000) + 1) * 1000;
    let max_batch_size = batch_size;
    let mut adaptive_batch_size = initial_batch_size
        .unwrap_or(max_batch_size)
        .clamp(1, max_batch_size);
    let mut consecutive_successes = 0usize;
    let mut reply_lookup_cache = HashMap::<ReplyLookupKey, Option<i64>>::new();
    let mut seen_message_rowids = HashMap::<(i64, i64), i64>::new();
    let mut prefetched_batch = None::<PrefetchedCompactBatchHandle>;

    loop {
        let desired_batch_size = remaining
            .map(|remaining| remaining.min(adaptive_batch_size))
            .unwrap_or(adaptive_batch_size);
        if desired_batch_size == 0 {
            break;
        }

        let (prepared_batch, effective_batch_size) = if let Some(prefetched_handle) = prefetched_batch.take() {
            let prefetched = join_prefetched_compact_batch(prefetched_handle);
            match prefetched.result {
                Ok(batch) => (
                    prepare_fast_compact_rows_light(batch),
                    prefetched.requested_batch_size,
                ),
                Err(err_msg) => {
                    let failing_batch_size = prefetched.requested_batch_size;
                    let Some(shrunk) = next_lower_adaptive_batch_size(failing_batch_size) else {
                        let fail_start = prefetched.lower_exclusive.saturating_add(1);
                        let fail_end = (last_rowid + failing_batch_size as i64).max(fail_start);
                        return Err(FastRangeFailure {
                            error: anyhow!(err_msg),
                            processed_rows: processed - initial_processed,
                            fail_start,
                            fail_end,
                        });
                    };
                    event!(
                        Level::WARN,
                        "表 {} 当前批大小 {} 失败，降到 {} 重试: {}",
                        table_name,
                        failing_batch_size,
                        shrunk,
                        err_msg
                    );
                    adaptive_batch_size = shrunk;
                    consecutive_successes = 0;
                    continue;
                }
            }
        } else {
            match fetch_fast_batch_compact_light(conn, &select_sql, last_rowid, None, desired_batch_size) {
                Ok(batch) => (prepare_fast_compact_rows_light(batch), desired_batch_size),
                Err(err) => {
                    let Some(shrunk) = next_lower_adaptive_batch_size(desired_batch_size) else {
                        let fail_start = last_rowid.saturating_add(1);
                        let fail_end = (last_rowid + desired_batch_size as i64).max(fail_start);
                        return Err(FastRangeFailure {
                            error: err,
                            processed_rows: processed - initial_processed,
                            fail_start,
                            fail_end,
                        });
                    };
                    event!(
                        Level::WARN,
                        "表 {} 当前批大小 {} 失败，降到 {} 重试: {}",
                        table_name,
                        desired_batch_size,
                        shrunk,
                        err
                    );
                    adaptive_batch_size = shrunk;
                    consecutive_successes = 0;
                    continue;
                }
            }
        };

        if prepared_batch.rows.is_empty() {
            break;
        }

        let batch_processed = prepared_batch.rows.len();
        let batch_max_rowid = prepared_batch.max_rowid;
        let should_prefetch_next = effective_batch_size >= 50_000
            && batch_processed == effective_batch_size
            && remaining
                .map(|remaining_rows| remaining_rows > batch_processed)
                .unwrap_or(true);
        let next_prefetch_handle = should_prefetch_next.then(|| {
            spawn_prefetch_compact_batch(
                db_path.clone(),
                select_sql.clone(),
                batch_max_rowid,
                effective_batch_size,
            )
        });

        let (decoded_batch, max_seen_rowid) = match finalize_prepared_fast_compact_batch(
            conn,
            table_name,
            prepared_batch,
            reference_lookup,
            &mut reply_lookup_cache,
            &mut seen_message_rowids,
        ) {
            Ok(prepared) => prepared,
            Err(err) => {
                consecutive_successes = 0;
                drop(next_prefetch_handle);
                let Some(shrunk) = next_lower_adaptive_batch_size(effective_batch_size) else {
                    let fail_start = last_rowid.saturating_add(1);
                    let fail_end = (last_rowid + effective_batch_size as i64).max(fail_start);
                    return Err(FastRangeFailure {
                        error: err,
                        processed_rows: processed - initial_processed,
                        fail_start,
                        fail_end,
                    });
                };
                event!(
                    Level::WARN,
                    "表 {} 当前批大小 {} 失败，降到 {} 重试: {}",
                    table_name,
                    effective_batch_size,
                    shrunk,
                    err
                );
                adaptive_batch_size = shrunk;
                continue;
            }
        };

        if let Err(err) = write_fast_compact_batch(conn, &quoted, &decoded_batch) {
            consecutive_successes = 0;
            let Some(shrunk) = next_lower_adaptive_batch_size(effective_batch_size) else {
                let fail_start = last_rowid.saturating_add(1);
                let fail_end = (last_rowid + effective_batch_size as i64).max(fail_start);
                return Err(FastRangeFailure {
                    error: err,
                    processed_rows: processed - initial_processed,
                    fail_start,
                    fail_end,
                });
            };
            event!(
                Level::WARN,
                "表 {} 当前批大小 {} 失败，降到 {} 重试: {}",
                table_name,
                effective_batch_size,
                shrunk,
                err
            );
            adaptive_batch_size = shrunk;
            continue;
        }

        prefetched_batch = next_prefetch_handle;
        last_rowid = max_seen_rowid;
        processed += batch_processed;
        if let Some(remaining_rows) = remaining.as_mut() {
            *remaining_rows = remaining_rows.saturating_sub(batch_processed);
        }
        if processed >= next_report {
            event!(Level::INFO, "表 {} 已写回 {} 条", table_name, processed);
            next_report = ((processed / 1_000) + 1) * 1_000;
        }

        consecutive_successes += 1;
        if consecutive_successes >= 2 && adaptive_batch_size < max_batch_size {
            if let Some(grown) = next_higher_adaptive_batch_size(adaptive_batch_size, max_batch_size) {
                adaptive_batch_size = grown;
            }
            consecutive_successes = 0;
        }
    }

    Ok(processed - initial_processed)
}

fn update_table_fast_range(
    conn: &Connection,
    table_name: &str,
    lower_exclusive: Option<i64>,
    upper_inclusive: Option<i64>,
    limit: Option<usize>,
    with_detail_json: bool,
    batch_size: usize,
    initial_batch_size: Option<usize>,
    reference_lookup: &references::ReferenceLookup,
    initial_processed: usize,
) -> std::result::Result<usize, FastRangeFailure> {
    if !with_detail_json && upper_inclusive.is_none() {
        if let Some(db_path) = conn.path().map(PathBuf::from) {
            return update_table_fast_range_compact_pipelined(
                conn,
                db_path,
                table_name,
                lower_exclusive,
                limit,
                batch_size,
                initial_batch_size,
                reference_lookup,
                initial_processed,
            );
        }
    }

    let quoted = quote_ident(table_name);
    let select_sql = if with_detail_json {
        if upper_inclusive.is_some() {
            format!(
                "SELECT rowid, Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 AND rowid <= ?2 ORDER BY rowid LIMIT ?3"
            )
        } else {
            format!(
                "SELECT rowid, Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2"
            )
        }
    } else {
        if upper_inclusive.is_some() {
            format!(
                "SELECT rowid, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 AND rowid <= ?2 ORDER BY rowid LIMIT ?3"
            )
        } else {
            format!(
                "SELECT rowid, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid > ?1 ORDER BY rowid LIMIT ?2"
            )
        }
    };

    let update_sql = if with_detail_json {
        format!(
            "UPDATE {quoted} SET DecodedMsg = ?1, SenderDisplay = ?2, DecodedDetailJson = ?3 WHERE rowid = ?4"
        )
    } else {
        format!("UPDATE {quoted} SET DecodedMsg = ?1, SenderDisplay = ?2 WHERE rowid = ?3")
    };
    let mut processed = initial_processed;
    let mut last_rowid = lower_exclusive.unwrap_or(0_i64);
    let mut remaining = limit;
    let mut next_report = ((processed / 1000) + 1) * 1000;
    let max_batch_size = batch_size;
    let mut adaptive_batch_size = initial_batch_size
        .unwrap_or(max_batch_size)
        .clamp(1, max_batch_size);
    let mut consecutive_successes = 0usize;
    let mut reply_lookup_cache = HashMap::<ReplyLookupKey, Option<i64>>::new();
    let mut seen_message_rowids = HashMap::<(i64, i64), i64>::new();

    loop {
        if let Some(upper) = upper_inclusive {
            if last_rowid >= upper {
                break;
            }
        }
        let current_batch_size = remaining
            .map(|remaining| remaining.min(adaptive_batch_size))
            .unwrap_or(adaptive_batch_size);
        if current_batch_size == 0 {
            break;
        }

        match process_fast_batch(
            conn,
            table_name,
            &quoted,
            &select_sql,
            &update_sql,
            last_rowid,
            upper_inclusive,
            current_batch_size,
            with_detail_json,
            reference_lookup,
            &mut reply_lookup_cache,
            &mut seen_message_rowids,
        ) {
            Ok(Some((batch_processed, max_seen_rowid))) => {
                last_rowid = max_seen_rowid;
                processed += batch_processed;
                if let Some(remaining_rows) = remaining.as_mut() {
                    *remaining_rows = remaining_rows.saturating_sub(batch_processed);
                }
                if processed >= next_report {
                    event!(Level::INFO, "表 {} 已写回 {} 条", table_name, processed);
                    next_report = ((processed / 1_000) + 1) * 1_000;
                }

                consecutive_successes += 1;
                if consecutive_successes >= 2 && adaptive_batch_size < max_batch_size {
                    if let Some(grown) =
                        next_higher_adaptive_batch_size(adaptive_batch_size, max_batch_size)
                    {
                        adaptive_batch_size = grown;
                    }
                    consecutive_successes = 0;
                }
            }
            Ok(None) => break,
            Err(err) => {
                let Some(shrunk) = next_lower_adaptive_batch_size(current_batch_size) else {
                    let fail_start = last_rowid.saturating_add(1);
                    let fail_end = upper_inclusive
                        .map(|upper| (last_rowid + current_batch_size as i64).min(upper))
                        .unwrap_or(last_rowid + current_batch_size as i64)
                        .max(fail_start);
                    return Err(FastRangeFailure {
                        error: err,
                        processed_rows: processed - initial_processed,
                        fail_start,
                        fail_end,
                    });
                };
                event!(
                    Level::WARN,
                    "表 {} 当前批大小 {} 失败，降到 {} 重试: {}",
                    table_name,
                    current_batch_size,
                    shrunk,
                    err
                );
                adaptive_batch_size = shrunk;
                consecutive_successes = 0;
                continue;
            }
        }
    }

    Ok(processed - initial_processed)
}

fn next_lower_adaptive_batch_size(current: usize) -> Option<usize> {
    const DESCENDING_STEPS: &[usize] = &[
        500_000, 200_000, 100_000, 50_000, 20_000, 10_000, 5_000, 2_000, 1_000, 500, 200, 100, 50,
        20, 10, 5, 2, 1,
    ];

    DESCENDING_STEPS
        .iter()
        .copied()
        .find(|&step| current > step)
}

fn next_higher_adaptive_batch_size(current: usize, max_batch_size: usize) -> Option<usize> {
    if current >= max_batch_size {
        return None;
    }

    const ASCENDING_STEPS: &[usize] = &[
        1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000,
        200_000, 500_000, 1_000_000,
    ];

    ASCENDING_STEPS
        .iter()
        .copied()
        .find(|&step| step > current && step <= max_batch_size)
        .or(Some(max_batch_size))
}

#[allow(clippy::too_many_arguments)]
fn process_fast_batch(
    conn: &Connection,
    table_name: &str,
    quoted_table: &str,
    select_sql: &str,
    update_sql: &str,
    last_rowid: i64,
    upper_inclusive: Option<i64>,
    current_batch_size: usize,
    with_detail_json: bool,
    reference_lookup: &references::ReferenceLookup,
    reply_lookup_cache: &mut HashMap<ReplyLookupKey, Option<i64>>,
    seen_message_rowids: &mut HashMap<(i64, i64), i64>,
) -> Result<Option<(usize, i64)>> {
    let read_conn = reference_lookup.main_conn().unwrap_or(conn);
    if with_detail_json {
        let batch = fetch_fast_batch(
            read_conn,
            select_sql,
            last_rowid,
            upper_inclusive,
            current_batch_size,
        )?;

        if batch.is_empty() {
            return Ok(None);
        }

        let mut decoded_batch = Vec::with_capacity(batch.len());
        for (rowid, time, rand, sender_uin, msg_content, info) in batch {
            let decoded = decode_for_writeback(
                read_conn,
                table_name,
                rowid,
                time,
                rand,
                sender_uin,
                &msg_content,
                &info,
                with_detail_json,
                reference_lookup,
            );
            decoded_batch.push((rowid, decoded));
        }
        let max_seen = decoded_batch
            .last()
            .map(|(rowid, _)| *rowid)
            .unwrap_or(last_rowid);
        let tx = conn.unchecked_transaction()?;
        {
            let mut update_stmt = tx.prepare_cached(update_sql)?;
            for (rowid, decoded) in &decoded_batch {
                update_stmt.execute((
                    &decoded.readable,
                    &decoded.sender_display,
                    &decoded.detail_json,
                    rowid,
                ))?;
            }
        }
        tx.commit()?;
        return Ok(Some((decoded_batch.len(), max_seen)));
    }

    let batch = fetch_fast_batch_compact_light(
        read_conn,
        select_sql,
        last_rowid,
        upper_inclusive,
        current_batch_size,
    )?;

    if batch.is_empty() {
        return Ok(None);
    }

    let (decoded_batch, max_seen) = prepare_fast_compact_batch_light(
        read_conn,
        table_name,
        batch,
        reference_lookup,
        reply_lookup_cache,
        seen_message_rowids,
    )?;
    write_fast_compact_batch(conn, quoted_table, &decoded_batch)?;

    Ok(Some((decoded_batch.len(), max_seen)))
}

fn update_table_resilient(
    conn: &Connection,
    table_name: &str,
    limit: Option<usize>,
    with_detail_json: bool,
    batch_size: usize,
    reference_lookup: &references::ReferenceLookup,
) -> Result<(usize, usize)> {
    let (min_rowid, max_rowid) = match table_rowid_bounds(conn, table_name) {
        Ok(bounds) => bounds,
        Err(err) => {
            event!(Level::WARN, "表 {} 无法读取，整表跳过: {}", table_name, err);
            return Ok((0, 1));
        }
    };
    let Some(min_rowid) = min_rowid else {
        return Ok((0, 0));
    };
    let Some(max_rowid) = max_rowid else {
        return Ok((0, 0));
    };

    let hard_limit = limit.unwrap_or(usize::MAX);
    let mut stats = ResilientStats::default();
    update_range_resilient(
        conn,
        table_name,
        min_rowid,
        max_rowid,
        hard_limit,
        with_detail_json,
        batch_size,
        None,
        reference_lookup,
        &mut stats,
    )?;
    Ok((stats.processed, stats.skipped))
}

fn update_table_resilient_from_failure(
    conn: &Connection,
    table_name: &str,
    failure: &FastRangeFailure,
    remaining_limit: Option<usize>,
    with_detail_json: bool,
    batch_size: usize,
    reference_lookup: &references::ReferenceLookup,
) -> Result<(usize, usize)> {
    let (_, max_rowid) = table_rowid_bounds(conn, table_name)?;
    let Some(max_rowid) = max_rowid else {
        return Ok((failure.processed_rows, 0));
    };
    if failure.fail_start > max_rowid {
        return Ok((failure.processed_rows, 0));
    }

    let mut stats = ResilientStats {
        processed: failure.processed_rows,
        skipped: 0,
    };
    let remaining = remaining_limit.unwrap_or(usize::MAX);
    if remaining == 0 {
        return Ok((stats.processed, stats.skipped));
    }

    // fast path 已经从大批次一路降到 1 才失败，直接从 1 开始逐行探测
    let safe_initial = 1_usize;
    update_range_resilient(
        conn,
        table_name,
        failure.fail_start,
        max_rowid,
        remaining,
        with_detail_json,
        batch_size,
        Some(safe_initial),
        reference_lookup,
        &mut stats,
    )?;
    Ok((stats.processed, stats.skipped))
}

fn table_rowid_bounds(conn: &Connection, table_name: &str) -> Result<(Option<i64>, Option<i64>)> {
    let quoted = quote_ident(table_name);
    let mut stmt = conn.prepare(&format!("SELECT MIN(rowid), MAX(rowid) FROM {quoted}"))?;
    Ok(stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?)))?)
}

#[derive(Default)]
struct ResilientStats {
    processed: usize,
    skipped: usize,
}

fn update_range_resilient(
    conn: &Connection,
    table_name: &str,
    start: i64,
    end: i64,
    remaining: usize,
    with_detail_json: bool,
    batch_size: usize,
    initial_batch_size: Option<usize>,
    reference_lookup: &references::ReferenceLookup,
    stats: &mut ResilientStats,
) -> Result<()> {
    if start > end || remaining == 0 {
        return Ok(());
    }

    match update_table_fast_range(
        conn,
        table_name,
        Some(start.saturating_sub(1)),
        Some(end),
        Some(remaining),
        with_detail_json,
        batch_size,
        initial_batch_size,
        reference_lookup,
        0,
    ) {
        Ok(processed) => {
            stats.processed += processed;
            Ok(())
        }
        Err(err) => {
            let FastRangeFailure {
                error,
                processed_rows,
                fail_start,
                fail_end,
            } = err;
            stats.processed += processed_rows;

            let remaining_after_prefix = remaining.saturating_sub(processed_rows);
            let fail_start = fail_start.clamp(start, end);
            let fail_end = fail_end.clamp(fail_start, end);

            event!(
                Level::WARN,
                "表 {} 在 rowid={}..{} 之间遇到损坏，切换到逐行探测恢复: {}",
                table_name,
                fail_start,
                fail_end,
                error
            );

            let processed_before_recovery_probe = stats.processed;
            let mut probe_rowid = fail_start;
            let mut resume_start = end.saturating_add(1);
            while probe_rowid <= end {
                match update_table_fast_range(
                    conn,
                    table_name,
                    Some(probe_rowid.saturating_sub(1)),
                    Some(probe_rowid),
                    Some(1),
                    with_detail_json,
                    1,
                    Some(1),
                    reference_lookup,
                    0,
                ) {
                    Ok(0) => break,
                    Ok(processed) => {
                        stats.processed += processed;
                        resume_start = probe_rowid.saturating_add(1);
                        event!(
                            Level::INFO,
                            "表 {} 在 rowid={} 重新找到正常行，从小批次恢复",
                            table_name,
                            probe_rowid
                        );
                        break;
                    }
                    Err(single_err) => {
                        stats.skipped += 1;
                        event!(
                            Level::WARN,
                            "表 {} 跳过损坏行 rowid={} (recovery probe failed: {})",
                            table_name,
                            probe_rowid,
                            single_err.error
                        );
                        probe_rowid = probe_rowid.saturating_add(1);
                        resume_start = probe_rowid;
                    }
                }
            }

            let consumed_recovery_probe = stats
                .processed
                .saturating_sub(processed_before_recovery_probe);
            if resume_start <= end {
                update_range_resilient(
                    conn,
                    table_name,
                    resume_start,
                    end,
                    remaining_after_prefix.saturating_sub(consumed_recovery_probe),
                    with_detail_json,
                    batch_size,
                    Some(next_higher_adaptive_batch_size(1, batch_size).unwrap_or(1)),
                    reference_lookup,
                    stats,
                )?;
            }
            Ok(())
        }
    }
}

fn ensure_decoded_columns(
    conn: &Connection,
    table_name: &str,
    with_detail_json: bool,
) -> Result<()> {
    ensure_text_column(conn, table_name, "DecodedMsg")?;
    ensure_text_column(conn, table_name, "SenderDisplay")?;
    if with_detail_json {
        ensure_text_column(conn, table_name, "DecodedDetailJson")?;
    }
    Ok(())
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

fn configure_read_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -131072;
PRAGMA mmap_size = 4294967296;
        PRAGMA cache_spill = OFF;
        ",
    )?;
    Ok(())
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

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn decode_for_writeback(
    conn: &Connection,
    table_name: &str,
    rowid: i64,
    time: i64,
    rand: i64,
    sender_uin: i64,
    msg_content: &[u8],
    info: &[u8],
    with_detail_json: bool,
    reference_lookup: &references::ReferenceLookup,
) -> DecodedWriteback {
    let reply_reference = raw::extract_reply_reference(msg_content);
    let reply_target_rowid = reply_reference.as_ref().and_then(|reply| {
        reference_lookup
            .reply_target_rowid_on(conn, table_name, reply)
            .ok()
            .flatten()
    });
    let raw_compact =
        match raw::RawData::new(time, rand, sender_uin, msg_content.to_vec(), info.to_vec())
            .decode_compact()
        {
            Ok(text) => text,
            Err(err) => format!("<decode error: {err}>"),
        };
    let readable = reference_lookup
        .merge_decoded(table_name, rowid, time, rand, info, &raw_compact)
        .unwrap_or_else(|_| raw_compact.clone());
    let readable = apply_reply_hint(readable, reply_reference.as_ref(), reply_target_rowid);
    let sender_display = reference_lookup
        .sender_display_name(sender_uin, info)
        .ok()
        .flatten();
    let detail_json = if with_detail_json {
        let raw_decoded =
            match raw::RawData::new(time, rand, sender_uin, msg_content.to_vec(), info.to_vec())
                .decode()
            {
                Ok(text) => text,
                Err(err) => format!("<decode error: {err}>"),
            };
        let index_text = reference_lookup
            .index_text(table_name, rowid)
            .ok()
            .flatten();
        let fold_prompt = reference_lookup
            .fold_prompt(table_name, time, rand, info)
            .ok()
            .flatten();
        let pubacc_name = reference_lookup.pubacc_name(sender_uin).ok().flatten();
        let (trace, trace_error) =
            match raw::RawData::new(time, rand, sender_uin, msg_content.to_vec(), info.to_vec())
                .trace()
            {
                Ok(trace) => (Some(trace), None),
                Err(err) => (None, Some(err.to_string())),
            };
        Some(
            serde_json::to_string(&DecodedDetailRecord {
                table: table_name.to_string(),
                rowid,
                time,
                rand,
                sender_uin,
                sender_display: sender_display.clone(),
                decoded_readable: readable.clone(),
                decoded_raw: raw_decoded,
                index_text,
                fold_prompt,
                pubacc_name,
                reply_reference,
                reply_target_rowid,
                trace,
                trace_error,
            })
            .unwrap_or_else(|err| {
                format!(
                    "{{\"table\":\"{}\",\"rowid\":{},\"error\":\"json serialize failed: {}\"}}",
                    table_name, rowid, err
                )
            }),
        )
    } else {
        None
    };

    DecodedWriteback {
        readable,
        sender_display,
        detail_json,
    }
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
