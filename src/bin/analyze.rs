use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{Connection, OptionalExtension};

#[path = "../elements.rs"]
mod elements;
#[path = "../raw.rs"]
mod raw;

#[derive(Debug)]
struct Options {
    db_path: PathBuf,
    index_db_path: Option<PathBuf>,
    table_like: Option<String>,
    sample_limit: usize,
}

#[derive(Debug, Clone)]
struct Example {
    table: String,
    rowid: i64,
    raw: String,
    index: Option<String>,
}

#[derive(Debug, Default)]
struct Bucket {
    count: usize,
    examples: Vec<Example>,
}

struct IndexLookup {
    conn: Connection,
}

fn main() -> Result<()> {
    let options = parse_args()?;
    let conn = Connection::open(&options.db_path)
        .with_context(|| format!("打开数据库失败: {:?}", options.db_path))?;
    let index_lookup = open_index_lookup(&options.db_path, options.index_db_path.as_ref())?;

    let mut tables = list_message_tables(&conn, options.table_like.as_deref())?;
    tables.sort();

    let mut buckets = BTreeMap::<String, Bucket>::new();
    let mut skipped = Vec::new();

    for table in tables {
        scan_table(
            &conn,
            &table,
            index_lookup.as_ref(),
            options.sample_limit,
            &mut buckets,
            &mut skipped,
        )?;
    }

    println!("=== 分类统计 ===");
    for (name, bucket) in buckets.iter().rev().collect::<Vec<_>>() {
        println!("{name}: {}", bucket.count);
        for example in &bucket.examples {
            println!(
                "  - {}#{} | raw={} | index={}",
                example.table,
                example.rowid,
                truncate(&example.raw, 100),
                truncate(example.index.as_deref().unwrap_or(""), 100)
            );
        }
    }

    println!("\n=== 跳过的损坏行 ===");
    if skipped.is_empty() {
        println!("无");
    } else {
        for item in skipped {
            println!("{item}");
        }
    }

    Ok(())
}

fn parse_args() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let db_path = PathBuf::from(args.next().ok_or_else(|| {
        anyhow!(
            "缺少数据库路径，用法: analyze <db_path> [--index-db PATH] [--table-like SUBSTR] [--sample-limit N]"
        )
    })?);

    let mut index_db_path = None;
    let mut table_like = None;
    let mut sample_limit = 3_usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--index-db" => {
                index_db_path = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--index-db 后面缺少数据库路径"))?,
                ));
            }
            "--table-like" => {
                table_like = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--table-like 后面缺少匹配字符串"))?,
                );
            }
            "--sample-limit" => {
                sample_limit = args
                    .next()
                    .ok_or_else(|| anyhow!("--sample-limit 后面缺少数字"))?
                    .parse::<usize>()
                    .context("无效的 --sample-limit")?;
            }
            other => bail!("未知参数: {other}"),
        }
    }

    Ok(Options {
        db_path,
        index_db_path,
        table_like,
        sample_limit,
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

fn open_index_lookup(db_path: &Path, cli_path: Option<&PathBuf>) -> Result<Option<IndexLookup>> {
    let candidate = if let Some(cli_path) = cli_path {
        Some(cli_path.clone())
    } else {
        let sibling = db_path.with_file_name("Msg3.0index.decrypted.db");
        if sibling.exists() {
            Some(sibling)
        } else {
            None
        }
    };

    let Some(index_path) = candidate else {
        return Ok(None);
    };

    if !index_path.exists() {
        return Ok(None);
    }

    let conn = Connection::open(&index_path)
        .with_context(|| format!("打开索引数据库失败: {:?}", index_path))?;
    Ok(Some(IndexLookup { conn }))
}

fn scan_table(
    conn: &Connection,
    table_name: &str,
    index_lookup: Option<&IndexLookup>,
    sample_limit: usize,
    buckets: &mut BTreeMap<String, Bucket>,
    skipped: &mut Vec<String>,
) -> Result<()> {
    let quoted = quote_ident(table_name);
    let mut stmt = conn.prepare(&format!(
        "SELECT MIN(rowid), MAX(rowid), COUNT(*) FROM {quoted}"
    ))?;
    let (min_rowid, max_rowid, count): (Option<i64>, Option<i64>, i64) =
        stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

    let Some(min_rowid) = min_rowid else {
        return Ok(());
    };
    let Some(max_rowid) = max_rowid else {
        return Ok(());
    };

    println!("扫描 {table_name} ({count} rows)");
    scan_range(
        conn,
        table_name,
        min_rowid,
        max_rowid,
        index_lookup,
        sample_limit,
        buckets,
        skipped,
    )
}

fn scan_range(
    conn: &Connection,
    table_name: &str,
    start: i64,
    end: i64,
    index_lookup: Option<&IndexLookup>,
    sample_limit: usize,
    buckets: &mut BTreeMap<String, Bucket>,
    skipped: &mut Vec<String>,
) -> Result<()> {
    let quoted = quote_ident(table_name);
    let sql = format!(
        "SELECT rowid, Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid BETWEEN ?1 AND ?2 ORDER BY rowid"
    );

    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(err) => {
            skipped.push(format!("{table_name}#{start}-{end}: prepare failed: {err}"));
            return Ok(());
        }
    };

    let rows = stmt.query_map((start, end), |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, Vec<u8>>(5)?,
        ))
    });

    match rows {
        Ok(rows) => {
            for row in rows {
                match row {
                    Ok((rowid, time, rand, sender_uin, msg_content, info)) => {
                        classify_row(
                            table_name,
                            rowid,
                            time,
                            rand,
                            sender_uin,
                            msg_content,
                            info,
                            index_lookup,
                            sample_limit,
                            buckets,
                        )?;
                    }
                    Err(err) => {
                        if start == end {
                            skipped.push(format!("{table_name}#{start}: {err}"));
                            return Ok(());
                        }
                        let mid = start + (end - start) / 2;
                        scan_range(
                            conn,
                            table_name,
                            start,
                            mid,
                            index_lookup,
                            sample_limit,
                            buckets,
                            skipped,
                        )?;
                        scan_range(
                            conn,
                            table_name,
                            mid + 1,
                            end,
                            index_lookup,
                            sample_limit,
                            buckets,
                            skipped,
                        )?;
                        return Ok(());
                    }
                }
            }
        }
        Err(err) => {
            if start == end {
                skipped.push(format!("{table_name}#{start}: {err}"));
                return Ok(());
            }
            let mid = start + (end - start) / 2;
            scan_range(
                conn,
                table_name,
                start,
                mid,
                index_lookup,
                sample_limit,
                buckets,
                skipped,
            )?;
            scan_range(
                conn,
                table_name,
                mid + 1,
                end,
                index_lookup,
                sample_limit,
                buckets,
                skipped,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn classify_row(
    table_name: &str,
    rowid: i64,
    time: i64,
    rand: i64,
    sender_uin: i64,
    msg_content: Vec<u8>,
    info: Vec<u8>,
    index_lookup: Option<&IndexLookup>,
    sample_limit: usize,
    buckets: &mut BTreeMap<String, Bucket>,
) -> Result<()> {
    let raw = match raw::RawData::new(time, rand, sender_uin, msg_content, info).decode() {
        Ok(text) => text,
        Err(err) => {
            push_bucket(
                buckets,
                "decode_error",
                sample_limit,
                Example {
                    table: table_name.to_string(),
                    rowid,
                    raw: format!("<decode error: {err}>"),
                    index: lookup_index(index_lookup, table_name, rowid)?,
                },
            );
            return Ok(());
        }
    };

    let category = classify_text(&raw);
    if let Some(category) = category {
        let index = lookup_index(index_lookup, table_name, rowid)?;
        push_bucket(
            buckets,
            &category,
            sample_limit,
            Example {
                table: table_name.to_string(),
                rowid,
                raw,
                index,
            },
        );
    }

    Ok(())
}

fn classify_text(raw: &str) -> Option<String> {
    let raw = raw.trim();

    if raw.is_empty() {
        return Some("raw_empty".to_string());
    }

    let generic_prefixes = [
        "[语音]",
        "[视频]",
        "[文件]",
        "[分享]",
        "[聊天记录]",
        "[表情]",
        "[群图片]",
        "[私聊图片]",
    ];

    for prefix in generic_prefixes {
        if raw == prefix || raw.starts_with(&format!("{prefix} ")) || raw.starts_with(prefix) {
            let mut label = format!("generic_{}", prefix.trim_matches(&['[', ']'][..]));
            label = label
                .replace("群图片", "group_image")
                .replace("私聊图片", "private_image")
                .replace("聊天记录", "chat_history")
                .replace("表情", "face")
                .replace("分享", "share")
                .replace("文件", "file")
                .replace("视频", "video")
                .replace("语音", "voice");
            return Some(label);
        }
    }

    None
}

fn push_bucket(
    buckets: &mut BTreeMap<String, Bucket>,
    name: &str,
    sample_limit: usize,
    example: Example,
) {
    let bucket = buckets.entry(name.to_string()).or_default();
    bucket.count += 1;
    if bucket.examples.len() < sample_limit {
        bucket.examples.push(example);
    }
}

fn lookup_index(
    index_lookup: Option<&IndexLookup>,
    table_name: &str,
    rowid: i64,
) -> Result<Option<String>> {
    let Some(index_lookup) = index_lookup else {
        return Ok(None);
    };
    index_lookup.lookup(table_name, rowid)
}

impl IndexLookup {
    fn lookup(&self, table_name: &str, rowid: i64) -> Result<Option<String>> {
        let Some((chat_type, account)) = table_to_index_scope(table_name) else {
            return Ok(None);
        };
        let mut stmt = self.conn.prepare(
            "SELECT c0msgcontent FROM msgindex_content WHERE c2chattype = ?1 AND c3account = ?2 AND c7rowidofmsg = ?3 LIMIT 1",
        )?;
        let text = stmt
            .query_row((chat_type, account, rowid), |row| row.get::<_, String>(0))
            .optional()?;
        Ok(text)
    }
}

fn table_to_index_scope(table_name: &str) -> Option<(i64, i64)> {
    if let Some(account) = table_name.strip_prefix("buddy_") {
        return account.parse::<i64>().ok().map(|account| (0, account));
    }
    if let Some(account) = table_name.strip_prefix("group_") {
        return account.parse::<i64>().ok().map(|account| (1, account));
    }
    if let Some(account) = table_name.strip_prefix("system_") {
        return account.parse::<i64>().ok().map(|account| (2, account));
    }
    None
}

fn truncate(text: &str, limit: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= limit {
        return text.to_string();
    }
    let prefix: String = chars.into_iter().take(limit).collect();
    format!("{prefix}...")
}
