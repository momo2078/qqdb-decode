use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::Connection;
use serde::Serialize;

#[path = "../elements.rs"]
mod elements;
#[path = "../raw.rs"]
mod raw;
#[path = "../references.rs"]
mod references;

struct Options {
    db_path: PathBuf,
    index_db_path: Option<PathBuf>,
    refs_dir: Option<PathBuf>,
    table: String,
    rowid: i64,
    json: bool,
}

#[derive(Debug, Serialize)]
struct RowDump {
    table: String,
    rowid: i64,
    time: i64,
    rand: i64,
    sender_uin: i64,
    decoded: String,
    raw_decoded: String,
    reply_reference: Option<raw::ReplyReference>,
    reply_target_rowid: Option<i64>,
    sender_display: Option<String>,
    index: Option<String>,
    fold_prompt: Option<String>,
    pubacc_name: Option<String>,
    msg_content_len: usize,
    info_len: usize,
    msg_content_hex_prefix: String,
    info_hex_prefix: String,
    trace: raw::MessageTrace,
}

fn main() -> Result<()> {
    let options = parse_args()?;
    let conn = Connection::open(&options.db_path)
        .with_context(|| format!("打开数据库失败: {:?}", options.db_path))?;
    let quoted = quote_ident(&options.table);
    let sql = format!(
        "SELECT Time, Rand, SenderUin, MsgContent, Info FROM {quoted} WHERE rowid = ?1 LIMIT 1"
    );
    let (time, rand, sender_uin, msg_content, info): (i64, i64, i64, Vec<u8>, Vec<u8>) = conn
        .query_row(&sql, [options.rowid], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .with_context(|| format!("查询 {}#{} 失败", options.table, options.rowid))?;

    let raw_decoded = raw::RawData::new(time, rand, sender_uin, msg_content.clone(), info.clone())
        .decode()
        .with_context(|| "raw decode failed")?;
    let reply_reference = raw::extract_reply_reference(&msg_content);
    let trace = raw::RawData::new(time, rand, sender_uin, msg_content.clone(), info.clone())
        .trace()
        .with_context(|| "trace decode failed")?;

    let reference_lookup = references::ReferenceLookup::open(
        &options.db_path,
        options.index_db_path.as_ref(),
        options.refs_dir.as_ref(),
    )?;
    let index = reference_lookup.index_text(&options.table, options.rowid)?;
    let fold_prompt = reference_lookup.fold_prompt(&options.table, time, rand, &info)?;
    let sender_display = reference_lookup.sender_display_name(sender_uin, &info)?;
    let pubacc_name = reference_lookup.pubacc_name(sender_uin)?;
    let reply_target_rowid = reply_reference.as_ref().and_then(|reply| {
        reference_lookup
            .reply_target_rowid(&options.table, reply)
            .ok()
            .flatten()
    });
    let decoded = reference_lookup
        .merge_decoded(
            &options.table,
            options.rowid,
            time,
            rand,
            &info,
            &raw_decoded,
        )
        .unwrap_or_else(|_| raw_decoded.clone());

    if options.json {
        let dump = RowDump {
            table: options.table,
            rowid: options.rowid,
            time,
            rand,
            sender_uin,
            decoded,
            raw_decoded,
            reply_reference,
            reply_target_rowid,
            sender_display,
            index,
            fold_prompt,
            pubacc_name,
            msg_content_len: msg_content.len(),
            info_len: info.len(),
            msg_content_hex_prefix: hex_string(&msg_content[..msg_content.len().min(512)]),
            info_hex_prefix: hex_string(&info[..info.len().min(512)]),
            trace,
        };
        println!("{}", serde_json::to_string_pretty(&dump)?);
        return Ok(());
    }

    println!("decoded={decoded}");
    println!("decoded_raw={raw_decoded}");
    println!(
        "reply_reference={}",
        serde_json::to_string(&reply_reference).unwrap_or_else(|_| "null".to_string())
    );
    println!(
        "reply_target_rowid={}",
        reply_target_rowid
            .map(|value| value.to_string())
            .unwrap_or_default()
    );
    println!("sender_display={}", sender_display.unwrap_or_default());
    println!("fold_prompt={}", fold_prompt.unwrap_or_default());
    println!("pubacc_name={}", pubacc_name.unwrap_or_default());
    println!("msg_content_len={}", msg_content.len());
    println!("info_len={}", info.len());
    println!(
        "msg_content_hex={}",
        hex_string(&msg_content[..msg_content.len().min(512)])
    );
    println!("info_hex={}", hex_string(&info[..info.len().min(512)]));
    println!("outer_payloads={}", trace.outer_payloads.len());
    for payload in trace.outer_payloads.iter().take(16) {
        println!(
            "  payload[{}] type=0x{:02x} label={} len={} preview={} hints={}",
            payload.index,
            payload.payload_type,
            payload.label,
            payload.length,
            payload.preview.as_deref().unwrap_or(""),
            payload.hints.join(" | ")
        );
    }
    println!("info_entries={}", trace.info_entries.len());
    for entry in trace.info_entries.iter().take(16) {
        println!(
            "  info[{}] tag=0x{:02x} key={} len={} text={} hints={}",
            entry.index,
            entry.tag,
            entry.key,
            entry.value_len,
            entry.text.as_deref().unwrap_or(""),
            entry.text_hints.join(" | ")
        );
    }
    println!("index={}", index.unwrap_or_default());

    Ok(())
}

fn parse_args() -> Result<Options> {
    let mut args = std::env::args().skip(1);
    let db_path = PathBuf::from(args.next().ok_or_else(|| {
        anyhow!("用法: decode_row <db_path> <table> <rowid> [--index-db PATH] [--refs-dir DIR]")
    })?);
    let table = args.next().ok_or_else(|| anyhow!("缺少 table 参数"))?;
    let rowid = args
        .next()
        .ok_or_else(|| anyhow!("缺少 rowid 参数"))?
        .parse::<i64>()
        .context("rowid 不是合法整数")?;
    let mut index_db_path = None;
    let mut refs_dir = None;
    let mut json = false;

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
            "--json" => json = true,
            other => bail!("未知参数: {other}"),
        }
    }

    Ok(Options {
        db_path,
        index_db_path,
        refs_dir,
        table,
        rowid,
        json,
    })
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn hex_string(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
