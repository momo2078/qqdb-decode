use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use regex::Regex;
use rusqlite::{params_from_iter, types::ValueRef, Connection, OpenFlags, OptionalExtension};

use crate::raw;

type ChatScope = (i64, i64);

#[derive(Debug, Default)]
struct ChatIndexCache {
    rows_by_rowid: Vec<CachedIndexRow>,
    rowid_by_seq: Vec<(u32, i64)>,
    text_pool: String,
}

#[derive(Debug, Clone, Copy)]
struct CachedIndexRow {
    rowid: i64,
    text_start: u32,
    text_len: u32,
}

#[derive(Debug, Default)]
pub struct ReferenceLookup {
    main: Option<Connection>,
    index: Option<Connection>,
    index_cache: Option<Arc<HashMap<ChatScope, ChatIndexCache>>>,
    fold: Option<Connection>,
    pubacc_names: Arc<HashMap<i64, String>>,
    loaded_sources: Vec<String>,
}

impl ReferenceLookup {
    pub fn open(
        db_path: &Path,
        cli_index_path: Option<&PathBuf>,
        refs_dir: Option<&PathBuf>,
    ) -> Result<Self> {
        let mut lookup = Self::default();
        let base_dir = refs_dir
            .cloned()
            .or_else(|| db_path.parent().map(Path::to_path_buf));

        if db_path.exists() {
            let conn = Connection::open_with_flags(
                db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
                .with_context(|| format!("打开主数据库失败: {:?}", db_path))?;
            raw::register_sqlite_functions(&conn)?;
            configure_main_lookup_connection(&conn)?;
            conn.set_prepared_statement_cache_capacity(64);
            lookup.main = Some(conn);
        }

        let index_path = if let Some(path) = cli_index_path {
            Some(path.clone())
        } else {
            base_dir
                .as_ref()
                .map(|dir| dir.join("Msg3.0index.decrypted.db"))
                .filter(|path| path.exists())
        };
        if let Some(path) = index_path {
            if path.exists() {
                let conn = Connection::open_with_flags(
                    &path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .with_context(|| format!("打开索引数据库失败: {:?}", path))?;
                configure_reference_connection(&conn)?;
                lookup.index_cache = Some(Arc::new(load_index_cache(&conn)?));
                lookup.loaded_sources.push(format!("索引数据库 {:?}", path));
            }
        }

        if let Some(dir) = base_dir.as_ref() {
            let fold_path = dir.join("FoldMsg.decrypted.db");
            if fold_path.exists() {
                let conn = Connection::open_with_flags(
                    &fold_path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .with_context(|| format!("打开折叠消息数据库失败: {:?}", fold_path))?;
                configure_reference_connection(&conn)?;
                lookup.fold = Some(conn);
                lookup
                    .loaded_sources
                    .push(format!("折叠消息数据库 {:?}", fold_path));
            }

            let pubacc_path = dir.join("PubAcc2.0.decrypted.db");
            if pubacc_path.exists() {
                let pubacc = Connection::open_with_flags(
                    &pubacc_path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )
                .with_context(|| format!("打开公众号数据库失败: {:?}", pubacc_path))?;
                configure_reference_connection(&pubacc)?;
                lookup.pubacc_names = Arc::new(load_pubacc_names(&pubacc)?);
                lookup
                    .loaded_sources
                    .push(format!("公众号数据库 {:?}", pubacc_path));
            }
        }

        Ok(lookup)
    }

    pub fn loaded_sources(&self) -> &[String] {
        &self.loaded_sources
    }

    pub fn main_conn(&self) -> Option<&Connection> {
        self.main.as_ref()
    }

    pub fn index_text(&self, table_name: &str, rowid: i64) -> Result<Option<String>> {
        let Some((chat_type, account)) = table_to_chat_scope(table_name) else {
            return Ok(None);
        };
        if let Some(index_cache) = self.index_cache.as_ref() {
            return Ok(index_cache
                .get(&(chat_type, account))
                .and_then(|cache| lookup_index_text_in_cache(cache, rowid)));
        }
        let Some(index) = self.index.as_ref() else {
            return Ok(None);
        };
        let mut stmt = index.prepare_cached(
            "SELECT c0msgcontent FROM msgindex_content WHERE c2chattype = ?1 AND c3account = ?2 AND c7rowidofmsg = ?3 AND c0msgcontent IS NOT NULL AND c0msgcontent != '' LIMIT 1",
        )?;
        let text = stmt
            .query_row((chat_type, account, rowid), |row| row.get::<_, String>(0))
            .optional()?;
        Ok(text)
    }

    pub fn prefetch_index_range(
        &self,
        table_name: &str,
        min_rowid: i64,
        max_rowid: i64,
    ) -> Result<HashMap<i64, String>> {
        let Some((chat_type, account)) = table_to_chat_scope(table_name) else {
            return Ok(HashMap::new());
        };
        if let Some(index_cache) = self.index_cache.as_ref() {
            return Ok(index_cache
                .get(&(chat_type, account))
                .map(|cache| prefetch_index_range_from_cache(cache, min_rowid, max_rowid))
                .unwrap_or_default());
        }
        let Some(index) = self.index.as_ref() else {
            return Ok(HashMap::new());
        };

        let mut stmt = index.prepare_cached(
            "SELECT c7rowidofmsg, c0msgcontent FROM msgindex_content WHERE c2chattype = ?1 AND c3account = ?2 AND c7rowidofmsg BETWEEN ?3 AND ?4 AND c0msgcontent IS NOT NULL AND c0msgcontent != ''",
        )?;
        let rows = stmt.query_map((chat_type, account, min_rowid, max_rowid), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut out =
            HashMap::with_capacity(max_rowid.saturating_sub(min_rowid).saturating_add(1) as usize);
        for row in rows {
            let (rowid, text) = row?;
            if !text.trim().is_empty() {
                out.insert(rowid, text);
            }
        }
        Ok(out)
    }

    pub fn fold_prompt(
        &self,
        table_name: &str,
        time: i64,
        rand: i64,
        info: &[u8],
    ) -> Result<Option<String>> {
        let Some(fold) = self.fold.as_ref() else {
            return Ok(None);
        };

        match table_to_chat_scope(table_name) {
            Some((0, account)) => {
                let mut stmt = fold.prepare_cached(
                    "SELECT prompt, toast FROM C2CFoldMsgInfo WHERE uin = ?1 AND msgTime = ?2 AND msgRandom = ?3 LIMIT 1",
                )?;
                let pair = stmt
                    .query_row((account, time, rand), |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, Option<String>>(1)?,
                        ))
                    })
                    .optional()?;
                Ok(pair.and_then(prefer_prompt_text))
            }
            Some((1, account)) => {
                if let Some(msg_seq) = raw::extract_msg_seq(info) {
                    let mut stmt = fold.prepare_cached(
                        "SELECT prompt, toast FROM GroupFoldMsgInfo WHERE groupId = ?1 AND msgSeq = ?2 AND msgRandom = ?3 LIMIT 1",
                    )?;
                    let pair = stmt
                        .query_row((account, msg_seq, rand), |row| {
                            Ok((
                                row.get::<_, Option<String>>(0)?,
                                row.get::<_, Option<String>>(1)?,
                            ))
                        })
                        .optional()?;
                    if let Some(text) = pair.and_then(prefer_prompt_text) {
                        return Ok(Some(text));
                    }
                }

                let mut stmt = fold.prepare_cached(
                    "SELECT prompt, toast FROM GroupFoldMsgInfo WHERE groupId = ?1 AND msgRandom = ?2 LIMIT 1",
                )?;
                let pair = stmt
                    .query_row((account, rand), |row| {
                        Ok((
                            row.get::<_, Option<String>>(0)?,
                            row.get::<_, Option<String>>(1)?,
                        ))
                    })
                    .optional()?;
                Ok(pair.and_then(prefer_prompt_text))
            }
            _ => Ok(None),
        }
    }

    pub fn sender_display_name(&self, sender_uin: i64, info: &[u8]) -> Result<Option<String>> {
        if let Some(name) = raw::extract_sender_show_name(info) {
            let name = name.trim();
            if !name.is_empty() {
                return Ok(Some(name.to_string()));
            }
        }

        if let Some(name) = self.pubacc_name(sender_uin)? {
            return Ok(Some(format!("{name}({sender_uin})")));
        }

        Ok(None)
    }

    pub fn merge_decoded(
        &self,
        table_name: &str,
        rowid: i64,
        time: i64,
        rand: i64,
        info: &[u8],
        raw_text: &str,
    ) -> Result<String> {
        let mut merged = raw_text.to_string();

        if let Some(index_text) = self.index_text(table_name, rowid)? {
            if !index_text.trim().is_empty() {
                merged = merge_preferred_text(&index_text, &merged);
            }
        }

        if should_use_fold_fallback(&merged) {
            if let Some(fold_prompt) = self.fold_prompt(table_name, time, rand, info)? {
                if !fold_prompt.trim().is_empty() {
                    merged = merge_preferred_text(&fold_prompt, &merged);
                }
            }
        }

        Ok(merged)
    }

    pub fn pubacc_name(&self, uin: i64) -> Result<Option<String>> {
        Ok(self.pubacc_names.get(&uin).cloned())
    }

    pub fn pubacc_names(&self) -> Arc<HashMap<i64, String>> {
        Arc::clone(&self.pubacc_names)
    }

    pub fn reply_target_rowid(
        &self,
        table_name: &str,
        reply: &raw::ReplyReference,
    ) -> Result<Option<i64>> {
        let Some(main) = self.main.as_ref() else {
            return Ok(None);
        };
        self.reply_target_rowid_on(main, table_name, reply)
    }

    pub fn reply_target_rowid_on(
        &self,
        main: &Connection,
        table_name: &str,
        reply: &raw::ReplyReference,
    ) -> Result<Option<i64>> {
        if reply.target_seq.is_none() && (reply.target_time.is_none() || reply.target_rand.is_none())
        {
            return Ok(None);
        }

        if let Some(target_rowid) = self.lookup_reply_rowid_by_time_rand(main, table_name, reply)? {
            return Ok(Some(target_rowid));
        }

        if let Some(target_rowid) = self.lookup_reply_rowid_by_seq(main, table_name, reply)? {
            return Ok(Some(target_rowid));
        }

        if let Some(target_rowid) = self.lookup_reply_rowid_in_index(table_name, reply)? {
            return Ok(Some(target_rowid));
        }

        Ok(None)
    }

    pub fn prefetch_reply_target_rowids_on(
        &self,
        main: &Connection,
        table_name: &str,
        replies: &[&raw::ReplyReference],
    ) -> Result<HashMap<(Option<u32>, Option<i64>, Option<i64>), Option<i64>>> {
        let mut key_to_rowid = HashMap::with_capacity(replies.len());
        let mut time_rand_to_keys =
            HashMap::<(i64, i64), Vec<(Option<u32>, Option<i64>, Option<i64>)>>::new();
        let mut seq_to_keys =
            HashMap::<u32, Vec<(Option<u32>, Option<i64>, Option<i64>)>>::new();

        for reply in replies {
            let key = (reply.target_seq, reply.target_time, reply.target_rand);
            if key_to_rowid.contains_key(&key) {
                continue;
            }
            key_to_rowid.insert(key, None);

            if let (Some(time), Some(rand)) = (reply.target_time, reply.target_rand) {
                time_rand_to_keys.entry((time, rand)).or_default().push(key);
            }
            if let Some(seq) = reply.target_seq {
                seq_to_keys.entry(seq).or_default().push(key);
            }
        }

        if !time_rand_to_keys.is_empty() {
            let pairs = time_rand_to_keys.keys().copied().collect::<Vec<_>>();
            let hits = self.lookup_rowids_in_main_by_time_rand_batch(main, table_name, &pairs)?;
            for (pair, keys) in time_rand_to_keys {
                if let Some(&rowid) = hits.get(&pair) {
                    for key in keys {
                        key_to_rowid.insert(key, Some(rowid));
                    }
                }
            }
        }

        let unresolved_main_seqs = seq_to_keys
            .iter()
            .filter(|(_, keys)| keys.iter().any(|key| key_to_rowid.get(key).copied().flatten().is_none()))
            .map(|(&seq, _)| seq)
            .collect::<Vec<_>>();
        if !unresolved_main_seqs.is_empty() {
            let hits =
                self.lookup_reply_rowids_by_seq_batch(main, table_name, &unresolved_main_seqs)?;
            for (seq, keys) in &seq_to_keys {
                if let Some(&rowid) = hits.get(seq) {
                    for &key in keys {
                        if key_to_rowid.get(&key).copied().flatten().is_none() {
                            key_to_rowid.insert(key, Some(rowid));
                        }
                    }
                }
            }
        }

        let unresolved_index_seqs = seq_to_keys
            .iter()
            .filter(|(_, keys)| keys.iter().any(|key| key_to_rowid.get(key).copied().flatten().is_none()))
            .map(|(&seq, _)| seq)
            .collect::<Vec<_>>();
        if !unresolved_index_seqs.is_empty() {
            let hits = self.lookup_reply_rowids_in_index_batch(table_name, &unresolved_index_seqs)?;
            for (seq, keys) in seq_to_keys {
                if let Some(&rowid) = hits.get(&seq) {
                    for key in keys {
                        if key_to_rowid.get(&key).copied().flatten().is_none() {
                            key_to_rowid.insert(key, Some(rowid));
                        }
                    }
                }
            }
        }

        Ok(key_to_rowid)
    }

    fn lookup_reply_rowid_by_seq(
        &self,
        main: &Connection,
        table_name: &str,
        reply: &raw::ReplyReference,
    ) -> Result<Option<i64>> {
        let Some(seq) = reply.target_seq else {
            return Ok(None);
        };
        let Some(seq_table_name) = reply_seq_table_name(table_name) else {
            return Ok(None);
        };

        let seq_sql = format!(
            "SELECT Time, Rand FROM {} WHERE Seq = ?1 LIMIT 1",
            quote_ident(&seq_table_name)
        );
        let pair = main
            .prepare_cached(&seq_sql)?
            .query_row([i64::from(seq)], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?;

        let Some((time, rand)) = pair else {
            return Ok(None);
        };
        self.lookup_rowid_in_main(main, table_name, time, rand)
    }

    fn lookup_reply_rowids_by_seq_batch(
        &self,
        main: &Connection,
        table_name: &str,
        seqs: &[u32],
    ) -> Result<HashMap<u32, i64>> {
        let Some(seq_table_name) = reply_seq_table_name(table_name) else {
            return Ok(HashMap::new());
        };
        let mut out = HashMap::with_capacity(seqs.len());
        for chunk in seqs.chunks(400) {
            let placeholders = sql_placeholders(chunk.len());
            let sql = format!(
                "SELECT s.Seq, m.rowid FROM {} s JOIN {} m ON m.Time = s.Time AND m.Rand = s.Rand WHERE s.Seq IN ({})",
                quote_ident(&seq_table_name),
                quote_ident(table_name),
                placeholders
            );
            let params = chunk.iter().map(|seq| i64::from(*seq)).collect::<Vec<_>>();
            let mut stmt = main.prepare_cached(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), |row| {
                Ok((row.get::<_, u32>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (seq, rowid) = row?;
                out.insert(seq, rowid);
            }
        }
        Ok(out)
    }

    fn lookup_reply_rowid_by_time_rand(
        &self,
        main: &Connection,
        table_name: &str,
        reply: &raw::ReplyReference,
    ) -> Result<Option<i64>> {
        let (Some(time), Some(rand)) = (reply.target_time, reply.target_rand) else {
            return Ok(None);
        };
        self.lookup_rowid_in_main(main, table_name, time, rand)
    }

    fn lookup_reply_rowid_in_index(
        &self,
        table_name: &str,
        reply: &raw::ReplyReference,
    ) -> Result<Option<i64>> {
        let Some((chat_type, account)) = table_to_chat_scope(table_name) else {
            return Ok(None);
        };
        let Some(seq) = reply.target_seq else {
            return Ok(None);
        };
        if let Some(index_cache) = self.index_cache.as_ref() {
            return Ok(index_cache
                .get(&(chat_type, account))
                .and_then(|cache| lookup_rowid_by_seq_in_cache(cache, seq)));
        }
        let Some(index) = self.index.as_ref() else {
            return Ok(None);
        };

        let mut stmt = index.prepare_cached(
            "SELECT c7rowidofmsg FROM msgindex_content WHERE c2chattype = ?1 AND c3account = ?2 AND c8seq = ?3 LIMIT 1",
        )?;
        let rowid = stmt
            .query_row((chat_type, account, i64::from(seq)), |row| {
                row.get::<_, i64>(0)
            })
            .optional()?;
        Ok(rowid)
    }

    fn lookup_reply_rowids_in_index_batch(
        &self,
        table_name: &str,
        seqs: &[u32],
    ) -> Result<HashMap<u32, i64>> {
        let Some((chat_type, account)) = table_to_chat_scope(table_name) else {
            return Ok(HashMap::new());
        };
        if let Some(index_cache) = self.index_cache.as_ref() {
            let mut out = HashMap::with_capacity(seqs.len());
            if let Some(cache) = index_cache.get(&(chat_type, account)) {
                for &seq in seqs {
                    if let Some(rowid) = lookup_rowid_by_seq_in_cache(cache, seq) {
                        out.insert(seq, rowid);
                    }
                }
            }
            return Ok(out);
        }
        let Some(index) = self.index.as_ref() else {
            return Ok(HashMap::new());
        };
        let mut out = HashMap::with_capacity(seqs.len());
        for chunk in seqs.chunks(400) {
            let placeholders = sql_placeholders(chunk.len());
            let sql = format!(
                "SELECT c8seq, c7rowidofmsg FROM msgindex_content WHERE c2chattype = ?1 AND c3account = ?2 AND c8seq IN ({})",
                placeholders
            );
            let mut params = Vec::with_capacity(chunk.len() + 2);
            params.push(chat_type);
            params.push(account);
            params.extend(chunk.iter().map(|seq| i64::from(*seq)));
            let mut stmt = index.prepare_cached(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), |row| {
                Ok((row.get::<_, u32>(0)?, row.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (seq, rowid) = row?;
                out.insert(seq, rowid);
            }
        }
        Ok(out)
    }

    fn lookup_rowid_in_main(
        &self,
        main: &Connection,
        table_name: &str,
        time: i64,
        rand: i64,
    ) -> Result<Option<i64>> {
        let sql = format!(
            "SELECT rowid FROM {} WHERE Time = ?1 AND Rand = ?2 LIMIT 1",
            quote_ident(table_name)
        );
        let rowid = main
            .prepare_cached(&sql)?
            .query_row((time, rand), |row| row.get::<_, i64>(0))
            .optional()?;
        Ok(rowid)
    }

    fn lookup_rowids_in_main_by_time_rand_batch(
        &self,
        main: &Connection,
        table_name: &str,
        pairs: &[(i64, i64)],
    ) -> Result<HashMap<(i64, i64), i64>> {
        let mut out = HashMap::with_capacity(pairs.len());
        for chunk in pairs.chunks(256) {
            let mut sql = format!(
                "SELECT Time, Rand, rowid FROM {} WHERE (Time, Rand) IN (",
                quote_ident(table_name)
            );
            let mut params = Vec::with_capacity(chunk.len() * 2);
            for (index, (time, rand)) in chunk.iter().enumerate() {
                if index > 0 {
                    sql.push(',');
                }
                sql.push_str("(?, ?)");
                params.push(*time);
                params.push(*rand);
            }
            sql.push(')');
            let mut stmt = main.prepare_cached(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            for row in rows {
                let (time, rand, rowid) = row?;
                out.insert((time, rand), rowid);
            }
        }
        Ok(out)
    }

}

fn sql_placeholders(count: usize) -> String {
    std::iter::repeat("?")
        .take(count)
        .collect::<Vec<_>>()
        .join(",")
}

pub fn merge_preferred_text(primary_text: &str, secondary_text: &str) -> String {
    let primary_trimmed = primary_text.trim();
    let secondary_trimmed = secondary_text.trim();
    if primary_trimmed.is_empty() {
        return secondary_trimmed.to_string();
    }
    if secondary_trimmed.is_empty() {
        return primary_trimmed.to_string();
    }
    if primary_trimmed == secondary_trimmed {
        return primary_trimmed.to_string();
    }

    let primary_text = normalize_display_text(primary_text);
    let secondary_text = normalize_display_text(secondary_text);
    let primary_text = primary_text.trim();
    let secondary_text = secondary_text.trim();

    if primary_text.is_empty() {
        return secondary_text.to_string();
    }
    if secondary_text.is_empty() {
        return primary_text.to_string();
    }
    if primary_text == secondary_text {
        return primary_text.to_string();
    }
    if let Some(merged) = merge_media_label_with_secondary_details(primary_text, secondary_text) {
        return merged;
    }
    let primary_compare = canonical_compare_text(primary_text);
    let secondary_compare = canonical_compare_text(secondary_text);
    if primary_compare == secondary_compare
        || (!primary_compare.is_empty() && secondary_compare.contains(&primary_compare))
        || (!secondary_compare.is_empty() && primary_compare.contains(&secondary_compare))
    {
        return prefer_better_text(primary_text, secondary_text).to_string();
    }
    if secondary_text.contains(primary_text) {
        return secondary_text.to_string();
    }
    if primary_text.contains(secondary_text) {
        return primary_text.to_string();
    }
    if generic_bracket_hint_matches(primary_text, secondary_text) {
        return secondary_text.to_string();
    }
    if same_bracket_prefix(primary_text, secondary_text) {
        return prefer_better_text(primary_text, secondary_text).to_string();
    }

    format!("{primary_text} {{{secondary_text}}}")
}

fn prefer_better_text<'a>(left: &'a str, right: &'a str) -> &'a str {
    let left_score = text_quality_score(left);
    let right_score = text_quality_score(right);
    if right_score > left_score {
        right
    } else if left_score > right_score {
        left
    } else if right.len() > left.len() {
        right
    } else {
        left
    }
}

fn text_quality_score(text: &str) -> i32 {
    let mut score = 0_i32;
    score += count_formatted_mentions(text) as i32 * 8;
    score -= text.matches("[符号表情]").count() as i32 * 10;
    score += emoji_like_count(text) as i32 * 6;
    score += if text.contains('(') && text.contains(')') {
        1
    } else {
        0
    };
    score
}

fn canonical_compare_text(text: &str) -> String {
    let mut text = if likely_needs_display_normalization(text) {
        normalize_display_text(text)
    } else {
        text.to_string()
    };
    if text.contains('@') && text.contains('(') && text.contains(')') {
        text = strip_mention_uins(&text);
    }
    if text.contains("[符号表情]") {
        text = text.replace("[符号表情]", "");
    }
    if contains_emoji_like(text.as_str()) {
        text = strip_emoji_like_chars(&text);
    }
    collapse_whitespace_owned(&text)
}

fn normalize_display_text(text: &str) -> String {
    if !likely_needs_display_normalization(text) {
        return text.to_string();
    }
    let text = rewrite_inline_mentions(text);
    collapse_leading_duplicate_mentions(&text)
}

fn rewrite_inline_mentions(text: &str) -> String {
    if !text.contains('@') {
        return text.to_string();
    }
    mention_raw_regex()
        .replace_all(text, "@$1($2)")
        .into_owned()
}

fn strip_mention_uins(text: &str) -> String {
    if !(text.contains('@') && text.contains('(') && text.contains(')')) {
        return text.to_string();
    }
    mention_formatted_regex()
        .replace_all(text, "@$name")
        .into_owned()
}

fn collapse_leading_duplicate_mentions(text: &str) -> String {
    let trimmed = text.trim_start();
    if !trimmed.starts_with('@') || !trimmed.contains('(') || !trimmed.contains(')') {
        return text.to_string();
    }
    let mut rest = text.trim_start();
    let leading_ws_len = text.len() - rest.len();
    let leading_ws = &text[..leading_ws_len];
    let mut mentions = Vec::new();

    while let Some(found) = mention_formatted_regex().find(rest) {
        if found.start() != 0 {
            break;
        }
        let token = found.as_str().to_string();
        if mentions.last() != Some(&token) {
            mentions.push(token);
        }
        let after = &rest[found.end()..];
        let trimmed = after.trim_start();
        if trimmed.len() == after.len() {
            rest = after;
            break;
        }
        rest = trimmed;
    }

    if mentions.is_empty() {
        return text.to_string();
    }

    if rest.is_empty() {
        format!("{leading_ws}{}", mentions.join(" "))
    } else {
        format!("{leading_ws}{} {}", mentions.join(" "), rest)
    }
}

fn count_formatted_mentions(text: &str) -> usize {
    if !(text.contains('@') && text.contains('(') && text.contains(')')) {
        return 0;
    }
    mention_formatted_regex().find_iter(text).count()
}

fn emoji_like_count(text: &str) -> usize {
    text.chars()
        .filter(|ch| {
            let code = *ch as u32;
            (0x1F300..=0x1FAFF).contains(&code) || (0x2600..=0x27BF).contains(&code)
        })
        .count()
}

fn contains_emoji_like(text: &str) -> bool {
    text.chars().any(|ch| {
        let code = ch as u32;
        (0x1F300..=0x1FAFF).contains(&code) || (0x2600..=0x27BF).contains(&code)
    })
}

fn strip_emoji_like_chars(text: &str) -> String {
    text.chars()
        .filter(|ch| {
            let code = *ch as u32;
            !((0x1F300..=0x1FAFF).contains(&code) || (0x2600..=0x27BF).contains(&code))
        })
        .collect()
}

fn mention_raw_regex() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| Regex::new(r"@([^@\r\n]+?)@(\d{5,})").expect("raw mention regex"))
}

fn mention_formatted_regex() -> &'static Regex {
    static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"@(?P<name>[^()\r\n]+?)\((?P<uin>\d{5,})\)").expect("formatted mention regex")
    })
}

fn likely_needs_display_normalization(text: &str) -> bool {
    text.contains('@')
}

fn collapse_whitespace_owned(text: &str) -> String {
    if !text.chars().any(char::is_whitespace) {
        return text.to_string();
    }

    let mut out = String::with_capacity(text.len());
    let mut in_whitespace = false;
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !in_whitespace {
                out.push(' ');
                in_whitespace = true;
            }
        } else {
            out.push(ch);
            in_whitespace = false;
        }
    }
    out.trim().to_string()
}

fn generic_bracket_hint_matches(primary_text: &str, secondary_text: &str) -> bool {
    let Some(label) = bracket_label(primary_text) else {
        return false;
    };
    if secondary_text.contains(label) {
        return true;
    }
    if let Some(core) = label.strip_suffix("消息") {
        if !core.is_empty() && secondary_text.contains(core) {
            return true;
        }
    }
    false
}

fn prefer_prompt_text(pair: (Option<String>, Option<String>)) -> Option<String> {
    let (prompt, toast) = pair;
    prompt
        .filter(|text| !text.trim().is_empty())
        .or_else(|| toast.filter(|text| !text.trim().is_empty()))
}

fn should_use_fold_fallback(text: &str) -> bool {
    let text = text.trim();
    text.is_empty() || text.starts_with("[未解析消息:") || text.starts_with("<decode error:")
}

fn same_bracket_prefix(primary_text: &str, secondary_text: &str) -> bool {
    let Some((primary_label, primary_rest)) = split_bracket_prefix(primary_text) else {
        return false;
    };
    let Some((secondary_label, secondary_rest)) = split_bracket_prefix(secondary_text) else {
        return false;
    };

    if primary_label != secondary_label {
        return false;
    }

    primary_rest.is_empty()
        || secondary_rest.is_empty()
        || secondary_rest.contains(primary_rest)
        || primary_rest.contains(secondary_rest)
}

fn split_bracket_prefix(text: &str) -> Option<(&str, &str)> {
    let text = text.trim();
    let body = text.strip_prefix('[')?;
    let comma_pos = body.find(',');
    let close_pos = body.find(']')?;
    let label_end = match comma_pos {
        Some(comma_pos) if comma_pos < close_pos => comma_pos,
        _ => close_pos,
    };

    let label = body[..label_end].trim();
    if label.is_empty() {
        return None;
    }

    Some((label, body[close_pos + 1..].trim()))
}

fn bracket_label(text: &str) -> Option<&str> {
    let text = text.trim();
    let body = text.strip_prefix('[')?;
    let close_pos = body.find(']')?;
    let label = body[..close_pos].trim();
    if label.is_empty() {
        return None;
    }
    Some(label)
}

fn merge_media_label_with_secondary_details(
    primary_text: &str,
    secondary_text: &str,
) -> Option<String> {
    let (primary_label, primary_rest) = split_bracket_prefix(primary_text)?;
    if !primary_rest.is_empty() {
        return None;
    }

    let secondary_label = bracket_label(secondary_text)?;
    if primary_label == secondary_label {
        return None;
    }

    let details = extract_bracket_details(secondary_text)?;
    if details.is_empty() {
        return None;
    }

    let details = strip_redundant_name(details, primary_label);
    if details.is_empty() {
        Some(format!("[{primary_label}]"))
    } else {
        Some(format!("[{primary_label}]{{{details}}}"))
    }
}

fn extract_bracket_details(text: &str) -> Option<&str> {
    let text = text.trim();
    let body = text.strip_prefix('[')?;
    let close_pos = body.find(']')?;
    let bracket_body = &body[..close_pos];
    if let Some(comma_pos) = bracket_body.find(',') {
        let details = bracket_body[comma_pos + 1..].trim();
        if !details.is_empty() {
            return Some(details);
        }
    }

    let rest = body[close_pos + 1..].trim();
    if let Some(inner) = rest.strip_prefix('{').and_then(|rest| rest.strip_suffix('}')) {
        let inner = inner.trim();
        if !inner.is_empty() {
            return Some(inner);
        }
    }
    if rest.starts_with("path=")
        || rest.starts_with("name=")
        || rest.starts_with("hash=")
        || rest.starts_with("size=")
    {
        return Some(rest);
    }
    None
}

fn strip_redundant_name<'a>(details: &'a str, primary_label: &str) -> &'a str {
    let prefix = format!("name={primary_label},");
    if details.starts_with(&prefix) {
        return details[prefix.len()..].trim();
    }
    let exact = format!("name={primary_label}");
    if details == exact {
        return "";
    }
    details
}

fn table_to_chat_scope(table_name: &str) -> Option<(i64, i64)> {
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

fn reply_seq_table_name(table_name: &str) -> Option<String> {
    table_name
        .strip_prefix("group_")
        .map(|account| format!("group${account}$Seq$"))
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn load_pubacc_names(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut stmt = conn.prepare(
        "SELECT uint64_pubacc_uin, str_pubacc_name FROM table_pubacc_basicinfo WHERE str_pubacc_name IS NOT NULL AND str_pubacc_name != ''",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut out = HashMap::new();
    for row in rows {
        let (uin, name) = row?;
        out.insert(uin, name);
    }
    Ok(out)
}

fn load_index_cache(conn: &Connection) -> Result<HashMap<ChatScope, ChatIndexCache>> {
    let mut stmt = conn.prepare(
        "SELECT c2chattype, c3account, c7rowidofmsg, c8seq, c0msgcontent \
         FROM msgindex_content \
         WHERE c0msgcontent IS NOT NULL AND c0msgcontent != ''",
    )?;
    let mut cache = HashMap::<ChatScope, ChatIndexCache>::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let chat_type = row.get::<_, i64>(0)?;
        let account = row.get::<_, i64>(1)?;
        let rowid = row.get::<_, i64>(2)?;
        let seq = row.get::<_, Option<i64>>(3)?;
        let text_ref = row.get_ref(4)?;
        let text = match text_ref {
            ValueRef::Text(bytes) => std::str::from_utf8(bytes)
                .with_context(|| "msgindex_content.c0msgcontent 不是合法 UTF-8")?,
            _ => continue,
        };
        let entry = cache.entry((chat_type, account)).or_default();
        let text_start = u32::try_from(entry.text_pool.len())
            .with_context(|| format!("索引缓存文本池过大: ({chat_type}, {account})"))?;
        entry.text_pool.push_str(text);
        let text_len = u32::try_from(text.len())
            .with_context(|| format!("索引缓存文本过长: ({chat_type}, {account})"))?;
        entry.rows_by_rowid.push(CachedIndexRow {
            rowid,
            text_start,
            text_len,
        });
        if let Some(seq) = seq.and_then(|value| u32::try_from(value).ok()) {
            entry.rowid_by_seq.push((seq, rowid));
        }
    }
    for entry in cache.values_mut() {
        entry.rows_by_rowid.sort_unstable_by_key(|row| row.rowid);
        entry.rowid_by_seq.sort_by_key(|(seq, _)| *seq);
        entry.rowid_by_seq.dedup_by_key(|(seq, _)| *seq);
    }
    Ok(cache)
}

fn lookup_index_text_in_cache(cache: &ChatIndexCache, rowid: i64) -> Option<String> {
    let index = cache
        .rows_by_rowid
        .binary_search_by_key(&rowid, |cached| cached.rowid)
        .ok()?;
    cached_index_text(cache, cache.rows_by_rowid[index]).map(str::to_string)
}

fn prefetch_index_range_from_cache(
    cache: &ChatIndexCache,
    min_rowid: i64,
    max_rowid: i64,
) -> HashMap<i64, String> {
    if min_rowid > max_rowid {
        return HashMap::new();
    }
    let start = cache
        .rows_by_rowid
        .partition_point(|row| row.rowid < min_rowid);
    let end = cache
        .rows_by_rowid
        .partition_point(|row| row.rowid <= max_rowid);
    let mut out = HashMap::with_capacity(end.saturating_sub(start));
    for &row in &cache.rows_by_rowid[start..end] {
        if let Some(text) = cached_index_text(cache, row) {
            out.insert(row.rowid, text.to_string());
        }
    }
    out
}

fn cached_index_text(cache: &ChatIndexCache, row: CachedIndexRow) -> Option<&str> {
    let start = usize::try_from(row.text_start).ok()?;
    let len = usize::try_from(row.text_len).ok()?;
    let end = start.checked_add(len)?;
    cache.text_pool.get(start..end)
}

fn lookup_rowid_by_seq_in_cache(cache: &ChatIndexCache, seq: u32) -> Option<i64> {
    let index = cache
        .rowid_by_seq
        .binary_search_by_key(&seq, |(cached_seq, _)| *cached_seq)
        .ok()?;
    Some(cache.rowid_by_seq[index].1)
}

fn configure_reference_connection(conn: &Connection) -> Result<()> {
    conn.set_prepared_statement_cache_capacity(256);
    conn.execute_batch(
        "
        PRAGMA query_only = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -131072;
PRAGMA mmap_size = 4294967296;
        PRAGMA cache_spill = OFF;
        ",
    )?;
    Ok(())
}

fn configure_main_lookup_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA query_only = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -32768;
PRAGMA mmap_size = 4294967296;
        PRAGMA cache_spill = OFF;
        ",
    )?;
    Ok(())
}

