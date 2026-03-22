use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;

#[path = "../elements.rs"]
mod elements;
#[path = "../raw.rs"]
mod raw;

#[derive(Debug)]
struct Options {
    db_path: PathBuf,
    index_db_path: Option<PathBuf>,
    output_dir: PathBuf,
    sample_limit: usize,
}

#[derive(Debug, Clone, Copy)]
struct CoreTarget {
    category: &'static str,
    sample_id: &'static str,
    table: &'static str,
    rowid: i64,
    focus_fields: &'static [&'static str],
    notes: &'static [&'static str],
}

#[derive(Debug, Default)]
struct Bucket {
    count: usize,
    samples: Vec<ManifestSample>,
}

#[derive(Debug, Serialize, Clone)]
struct ManifestSample {
    sample_id: String,
    category: String,
    table: String,
    rowid: i64,
    time: i64,
    rand: i64,
    sender_uin: i64,
    outer_types: Vec<u8>,
    decoded_now: String,
    index_text: Option<String>,
    msg_content_len: usize,
    info_len: usize,
    msg_content_prefix_hex: String,
    info_prefix_hex: String,
    focus_fields: Vec<String>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SampleManifest {
    generated_at_unix: u64,
    db_path: String,
    index_db_path: Option<String>,
    target_modules: Vec<String>,
    core_samples: Vec<ManifestSample>,
    supplemental: BTreeMap<String, Vec<ManifestSample>>,
}

#[derive(Debug, Serialize)]
struct FieldCategoryTemplate {
    name: String,
    runtime_priority: bool,
    target_fields: Vec<String>,
    sample_ids: Vec<String>,
    evidence: Vec<String>,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct FieldMapTemplate {
    generated_at_unix: u64,
    categories: Vec<FieldCategoryTemplate>,
}

struct IndexLookup {
    conn: Connection,
}

const CORE_TARGETS: &[CoreTarget] = &[
    CoreTarget {
        category: "share_0x1b",
        sample_id: "core_share_0x1b",
        table: "group_771939755",
        rowid: 62,
        focus_fields: &["title", "summary", "url", "source", "app"],
        notes: &[
            "首批分享卡片样本",
            "优先确认 MsgContent 内嵌卡片体与运行时拼接关系",
        ],
    },
    CoreTarget {
        category: "forward_0x1e",
        sample_id: "core_forward_0x1e",
        table: "group_869413256",
        rowid: 3694,
        focus_fields: &["count", "sender", "first_item_summary", "item_kind"],
        notes: &[
            "首批聊天记录样本",
            "重点盯 buffMsgPackListStream / arrPackets / arrSeqList",
        ],
    },
    CoreTarget {
        category: "meta_0x19",
        sample_id: "core_meta_0x19",
        table: "group_3929544273",
        rowid: 2689,
        focus_fields: &["should_ignore", "fallback_text"],
        notes: &["首批空白消息样本", "运行时只判定应忽略还是漏解正文"],
    },
];

fn main() -> Result<()> {
    let options = parse_args()?;
    fs::create_dir_all(&options.output_dir)
        .with_context(|| format!("创建输出目录失败: {:?}", options.output_dir))?;

    let conn = Connection::open(&options.db_path)
        .with_context(|| format!("打开数据库失败: {:?}", options.db_path))?;
    let index_lookup = open_index_lookup(&options.db_path, options.index_db_path.as_ref())?;

    let core_samples = CORE_TARGETS
        .iter()
        .map(|target| {
            fetch_sample(
                &conn,
                target.table,
                target.rowid,
                index_lookup.as_ref(),
                target.sample_id,
                target.category,
                target.focus_fields,
                target.notes,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let mut tables = list_message_tables(&conn, None)?;
    tables.sort();

    let mut buckets = BTreeMap::<String, Bucket>::new();
    for table in tables {
        scan_table(
            &conn,
            &table,
            index_lookup.as_ref(),
            options.sample_limit,
            &mut buckets,
        )?;
    }

    let supplemental = buckets
        .into_iter()
        .map(|(name, bucket)| (name, bucket.samples))
        .collect::<BTreeMap<_, _>>();

    let generated_at_unix = now_unix();
    let manifest = SampleManifest {
        generated_at_unix,
        db_path: options.db_path.display().to_string(),
        index_db_path: options
            .index_db_path
            .as_ref()
            .map(|path| path.display().to_string()),
        target_modules: vec![
            "KernelUtil.dll".to_string(),
            "IM.dll".to_string(),
            "MsgMgr.dll".to_string(),
            "VQQProto.dll".to_string(),
        ],
        core_samples: core_samples.clone(),
        supplemental,
    };

    let field_map = build_field_map_template(&manifest);
    let validation_report = build_validation_report(&manifest);

    fs::write(
        options.output_dir.join("sample_manifest.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;
    fs::write(
        options.output_dir.join("field_map.json"),
        serde_json::to_string_pretty(&field_map)?,
    )?;
    fs::write(
        options.output_dir.join("field_map.md"),
        build_field_map_markdown(&field_map),
    )?;
    fs::write(
        options.output_dir.join("validation_report.md"),
        validation_report,
    )?;
    ensure_oracle_log(&options.output_dir)?;

    println!("已生成运行时准备文件:");
    println!(
        "  {}",
        options.output_dir.join("sample_manifest.json").display()
    );
    println!("  {}", options.output_dir.join("field_map.json").display());
    println!("  {}", options.output_dir.join("field_map.md").display());
    println!(
        "  {}",
        options.output_dir.join("validation_report.md").display()
    );
    println!(
        "  {}",
        options.output_dir.join("oracle_log.jsonl").display()
    );

    Ok(())
}

fn parse_args() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let db_path = PathBuf::from(args.next().ok_or_else(|| {
        anyhow!(
            "缺少数据库路径，用法: runtime_prep <db_path> [--index-db PATH] [--output-dir DIR] [--sample-limit N]"
        )
    })?);

    let mut index_db_path = None;
    let mut output_dir = PathBuf::from("runtime_artifacts");
    let mut sample_limit = 5_usize;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--index-db" => {
                index_db_path = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--index-db 后面缺少数据库路径"))?,
                ));
            }
            "--output-dir" => {
                output_dir = PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--output-dir 后面缺少目录路径"))?,
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
        output_dir,
        sample_limit,
    })
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

fn fetch_sample(
    conn: &Connection,
    table: &str,
    rowid: i64,
    index_lookup: Option<&IndexLookup>,
    sample_id: &str,
    category: &str,
    focus_fields: &[&str],
    notes: &[&str],
) -> Result<ManifestSample> {
    let quoted = quote_ident(table);
    let sql = format!(
        "SELECT Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid = ?1 LIMIT 1"
    );
    let (time, rand, sender_uin, msg_content, info): (i64, i64, i64, Vec<u8>, Vec<u8>) = conn
        .query_row(&sql, [rowid], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .with_context(|| format!("查询样本 {}#{} 失败", table, rowid))?;

    let raw = raw::RawData::new(time, rand, sender_uin, msg_content.clone(), info.clone());
    let decoded_now = raw
        .decode()
        .unwrap_or_else(|err| format!("<decode error: {err}>"));
    let trace = raw
        .trace()
        .with_context(|| format!("trace 样本 {}#{} 失败", table, rowid))?;
    let index_text = lookup_index(index_lookup, table, rowid)?;

    Ok(ManifestSample {
        sample_id: sample_id.to_string(),
        category: category.to_string(),
        table: table.to_string(),
        rowid,
        time,
        rand,
        sender_uin,
        outer_types: trace
            .outer_payloads
            .iter()
            .map(|payload| payload.payload_type)
            .collect(),
        decoded_now,
        index_text,
        msg_content_len: msg_content.len(),
        info_len: info.len(),
        msg_content_prefix_hex: hex_prefix(&msg_content, 96),
        info_prefix_hex: hex_prefix(&info, 96),
        focus_fields: focus_fields.iter().map(|field| field.to_string()).collect(),
        notes: notes.iter().map(|note| note.to_string()).collect(),
    })
}

fn scan_table(
    conn: &Connection,
    table_name: &str,
    index_lookup: Option<&IndexLookup>,
    sample_limit: usize,
    buckets: &mut BTreeMap<String, Bucket>,
) -> Result<()> {
    let quoted = quote_ident(table_name);
    let mut stmt = conn.prepare(&format!(
        "SELECT MIN(rowid), MAX(rowid), COUNT(*) FROM {quoted}"
    ))?;
    let (min_rowid, max_rowid, _count): (Option<i64>, Option<i64>, i64) =
        stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    let Some(min_rowid) = min_rowid else {
        return Ok(());
    };
    let Some(max_rowid) = max_rowid else {
        return Ok(());
    };

    scan_range(
        conn,
        table_name,
        min_rowid,
        max_rowid,
        index_lookup,
        sample_limit,
        buckets,
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
) -> Result<()> {
    let quoted = quote_ident(table_name);
    let sql = format!(
        "SELECT rowid, Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid BETWEEN ?1 AND ?2 ORDER BY rowid"
    );
    let mut stmt = match conn.prepare(&sql) {
        Ok(stmt) => stmt,
        Err(_) => return Ok(()),
    };
    let mut rows = match stmt.query((start, end)) {
        Ok(rows) => rows,
        Err(_) => {
            if start == end {
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
            )?;
            scan_range(
                conn,
                table_name,
                mid + 1,
                end,
                index_lookup,
                sample_limit,
                buckets,
            )?;
            return Ok(());
        }
    };

    loop {
        let row = match rows.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(_) => {
                if start == end {
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
                )?;
                scan_range(
                    conn,
                    table_name,
                    mid + 1,
                    end,
                    index_lookup,
                    sample_limit,
                    buckets,
                )?;
                return Ok(());
            }
        };

        let rowid: i64 = match row.get(0) {
            Ok(value) => value,
            Err(_) => {
                if start == end {
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
                )?;
                scan_range(
                    conn,
                    table_name,
                    mid + 1,
                    end,
                    index_lookup,
                    sample_limit,
                    buckets,
                )?;
                return Ok(());
            }
        };
        let time: i64 = match row.get(1) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let rand: i64 = match row.get(2) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let sender_uin: i64 = match row.get(3) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let msg_content: Vec<u8> = match row.get(4) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let info: Vec<u8> = match row.get(5) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let raw_data = raw::RawData::new(time, rand, sender_uin, msg_content.clone(), info.clone());
        let decoded_now = match raw_data.decode() {
            Ok(text) => text,
            Err(_) => continue,
        };
        let index_text = lookup_index(index_lookup, table_name, rowid)?;
        let categories = classify_sample_categories(&decoded_now, index_text.as_deref());

        for category in categories {
            let bucket = buckets.entry(category.clone()).or_default();
            bucket.count += 1;
            if bucket.samples.len() >= sample_limit {
                continue;
            }
            let trace = match raw_data.trace() {
                Ok(trace) => trace,
                Err(_) => continue,
            };
            bucket.samples.push(ManifestSample {
                sample_id: format!("{category}_{table_name}_{rowid}"),
                category,
                table: table_name.to_string(),
                rowid,
                time,
                rand,
                sender_uin,
                outer_types: trace
                    .outer_payloads
                    .iter()
                    .map(|payload| payload.payload_type)
                    .collect(),
                decoded_now: decoded_now.clone(),
                index_text: index_text.clone(),
                msg_content_len: msg_content.len(),
                info_len: info.len(),
                msg_content_prefix_hex: hex_prefix(&msg_content, 96),
                info_prefix_hex: hex_prefix(&info, 96),
                focus_fields: Vec::new(),
                notes: vec!["自动补充样本池".to_string()],
            });
        }
    }

    Ok(())
}

fn classify_sample_categories(decoded_now: &str, index_text: Option<&str>) -> Vec<String> {
    let raw = decoded_now.trim();
    let index = index_text.unwrap_or("").trim();
    let mut categories = Vec::new();

    if raw.is_empty() {
        categories.push("raw_empty".to_string());
    }
    if raw.starts_with("[分享]") {
        categories.push("generic_share".to_string());
    }
    if raw.starts_with("[聊天记录]") {
        categories.push("generic_chat_history".to_string());
    }
    if !raw.is_empty()
        && !index.is_empty()
        && raw != index
        && !raw.contains(index)
        && !index.contains(raw)
    {
        categories.push("index_diff".to_string());
    }

    categories
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

fn hex_prefix(data: &[u8], limit: usize) -> String {
    let mut out = String::with_capacity(data.len().min(limit) * 2);
    for byte in data.iter().take(limit) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn build_field_map_template(manifest: &SampleManifest) -> FieldMapTemplate {
    let core_ids = |name: &str| -> Vec<String> {
        manifest
            .core_samples
            .iter()
            .filter(|sample| sample.category == name)
            .map(|sample| sample.sample_id.clone())
            .collect()
    };

    FieldMapTemplate {
        generated_at_unix: manifest.generated_at_unix,
        categories: vec![
            FieldCategoryTemplate {
                name: "share_0x1b".to_string(),
                runtime_priority: true,
                target_fields: vec![
                    "title".to_string(),
                    "summary".to_string(),
                    "url".to_string(),
                    "source".to_string(),
                    "app".to_string(),
                ],
                sample_ids: core_ids("share_0x1b"),
                evidence: Vec::new(),
                notes: vec!["先确认 MsgContent 内嵌卡片体，再确认 Info 补充字段".to_string()],
            },
            FieldCategoryTemplate {
                name: "forward_0x1e".to_string(),
                runtime_priority: true,
                target_fields: vec![
                    "count".to_string(),
                    "sender".to_string(),
                    "first_item_summary".to_string(),
                    "item_kind".to_string(),
                ],
                sample_ids: core_ids("forward_0x1e"),
                evidence: Vec::new(),
                notes: vec!["重点跟 buffMsgPackListStream / arrPackets / arrSeqList".to_string()],
            },
            FieldCategoryTemplate {
                name: "meta_0x19".to_string(),
                runtime_priority: true,
                target_fields: vec!["should_ignore".to_string(), "fallback_text".to_string()],
                sample_ids: core_ids("meta_0x19"),
                evidence: Vec::new(),
                notes: vec!["只判定应忽略元数据还是漏解正文".to_string()],
            },
            FieldCategoryTemplate {
                name: "large_info_blobs".to_string(),
                runtime_priority: true,
                target_fields: vec![
                    "bytes_pb_reserve".to_string(),
                    "buffMsgPackListStream".to_string(),
                    "arrPackets".to_string(),
                    "arrSeqList".to_string(),
                ],
                sample_ids: manifest
                    .core_samples
                    .iter()
                    .map(|sample| sample.sample_id.clone())
                    .collect(),
                evidence: Vec::new(),
                notes: vec!["确认哪些字段真正参与显示，哪些只是存储态元数据".to_string()],
            },
        ],
    }
}

fn build_field_map_markdown(field_map: &FieldMapTemplate) -> String {
    let mut out = String::new();
    out.push_str("# field_map\n\n");
    out.push_str(&format!(
        "- generated_at_unix: {}\n\n",
        field_map.generated_at_unix
    ));

    for category in &field_map.categories {
        out.push_str(&format!("## {}\n\n", category.name));
        out.push_str(&format!(
            "- runtime_priority: {}\n",
            category.runtime_priority
        ));
        out.push_str(&format!(
            "- target_fields: {}\n",
            category.target_fields.join(", ")
        ));
        out.push_str(&format!(
            "- sample_ids: {}\n",
            category.sample_ids.join(", ")
        ));
        for note in &category.notes {
            out.push_str(&format!("- note: {}\n", note));
        }
        out.push_str("- evidence: \n\n");
    }

    out
}

fn build_validation_report(manifest: &SampleManifest) -> String {
    let mut out = String::new();
    out.push_str("# validation_report\n\n");
    out.push_str("## 摘要\n\n");
    out.push_str(&format!("- db_path: `{}`\n", manifest.db_path));
    if let Some(index_db_path) = manifest.index_db_path.as_deref() {
        out.push_str(&format!("- index_db_path: `{}`\n", index_db_path));
    }
    out.push_str(&format!(
        "- target_modules: {}\n\n",
        manifest.target_modules.join(", ")
    ));

    out.push_str("## 核心样本\n\n");
    for sample in &manifest.core_samples {
        out.push_str(&format!(
            "- `{}` => {}#{} | outer={:?} | decoded=`{}` | index=`{}`\n",
            sample.sample_id,
            sample.table,
            sample.rowid,
            sample.outer_types,
            sample.decoded_now.replace('`', "'"),
            sample
                .index_text
                .clone()
                .unwrap_or_default()
                .replace('`', "'"),
        ));
    }
    out.push('\n');

    out.push_str("## 自动补充样本池\n\n");
    for (name, samples) in &manifest.supplemental {
        out.push_str(&format!("### {}\n\n", name));
        out.push_str(&format!("- count(sampled): {}\n", samples.len()));
        for sample in samples {
            out.push_str(&format!(
                "  - {}#{} | outer={:?} | decoded=`{}` | index=`{}`\n",
                sample.table,
                sample.rowid,
                sample.outer_types,
                sample.decoded_now.replace('`', "'"),
                sample
                    .index_text
                    .clone()
                    .unwrap_or_default()
                    .replace('`', "'"),
            ));
        }
        out.push('\n');
    }

    out
}

fn ensure_oracle_log(output_dir: &Path) -> Result<()> {
    let path = output_dir.join("oracle_log.jsonl");
    if path.exists() {
        return Ok(());
    }
    let mut file =
        File::create(&path).with_context(|| format!("创建 oracle log 失败: {}", path.display()))?;
    file.write_all(b"")?;
    Ok(())
}
