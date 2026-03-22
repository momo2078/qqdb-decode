use std::io::Read;
use std::sync::OnceLock;

use anyhow::{anyhow, bail, Result};
use std::collections::HashMap;
use flate2::read::ZlibDecoder;
use regex::Regex;
use rusqlite::{functions::FunctionFlags, Connection, Error as SqlError};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::elements::TextElement;

const MSG_TEXT: u8 = 0x01;
const MSG_FACE: u8 = 0x02;
const MSG_GROUP_IMAGE: u8 = 0x03;
const MSG_PRIVATE_IMAGE: u8 = 0x06;
const MSG_VOICE: u8 = 0x07;
const MSG_EXT_IMAGE: u8 = 0x0c;
const MSG_EMOJI: u8 = 0x0d;
const MSG_TRANSFER: u8 = 0x11;
const MSG_NICKNAME: u8 = 0x12;
const MSG_SHARE: u8 = 0x14;
const MSG_FILE: u8 = 0x18;
const MSG_VIDEO: u8 = 0x1a;
const MSG_TD_27: u8 = 0x1b;
const MSG_TD_30: u8 = 0x1e;

const METADATA_TYPES: &[u8] = &[0x00, 0x0e, 0x12, 0x19];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawData {
    pub time: i64,
    pub rand: i64,
    pub sender_uin: i64,
    pub msg_content: Vec<u8>,
    pub info: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageHeaderTrace {
    pub time: u32,
    pub rand: u32,
    pub color: u32,
    pub font_size: u8,
    pub font_style: u8,
    pub charset: u8,
    pub font_family: u8,
    pub font_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OuterPayloadTrace {
    pub index: usize,
    pub offset: usize,
    pub payload_type: u8,
    pub label: String,
    pub length: usize,
    pub preview: Option<String>,
    pub hints: Vec<String>,
    pub raw_hex_prefix: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InfoEntryTrace {
    pub index: usize,
    pub tag: u8,
    pub key: String,
    pub value_len: usize,
    pub text: Option<String>,
    pub numeric_u32: Option<u32>,
    pub nested_keys: Vec<String>,
    pub text_hints: Vec<String>,
    pub raw_hex_prefix: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessageTrace {
    pub header: MessageHeaderTrace,
    pub decoded: String,
    pub part_previews: Vec<String>,
    pub outer_payloads: Vec<OuterPayloadTrace>,
    pub unknown_outer_types: Vec<u8>,
    pub info_entries: Vec<InfoEntryTrace>,
    pub info_fallback: Option<String>,
    pub reply_reference: Option<ReplyReference>,
}

#[derive(Debug, Default, Clone, Serialize, PartialEq, Eq)]
pub struct ReplyReference {
    pub target_seq: Option<u32>,
    pub target_sender_uin: Option<i64>,
    pub target_time: Option<i64>,
    pub target_rand: Option<i64>,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactDecodeContext {
    pub text: String,
    pub reply_reference: Option<ReplyReference>,
    pub sender_show_name: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct ParsedOuterPayload<'a> {
    offset: usize,
    payload_type: u8,
    payload: &'a [u8],
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct FileInfo {
    filename: String,
    size_text: String,
    size_bytes: Option<u64>,
    path: String,
    md5: String,
    extra_json: String,
    hints: Vec<String>,
}

#[derive(Debug, Default, Clone)]
struct ExtImageInfo {
    name: Option<String>,
    width: Option<u16>,
    height: Option<u16>,
}

#[derive(Debug, Default, Clone)]
struct InfoValue {
    raw: Vec<u8>,
    text: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct InfoMap {
    entries: HashMap<String, InfoValue>,
}

struct LazyInfoMap<'a> {
    raw: &'a [u8],
    parsed: Option<Option<InfoMap>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DecodedMessage {
    parts: Vec<DecodedPart>,
    unknown_outer_types: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodedPart {
    Text(String),
    Face(String),
    Emoji(String),
    Image(ImagePart),
    Voice(VoicePart),
    Video(VideoPart),
    File(FileInfo),
    Share(SharePart),
    ForwardRecord(ForwardRecordPart),
    Transfer(TransferPart),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ImagePart {
    label: String,
    name: Option<String>,
    summary: Option<String>,
    path: Option<String>,
    hash: Option<String>,
    size_text: Option<String>,
    customface_type: Option<u32>,
    biz_type: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct VoicePart {
    preview: String,
    path: Option<String>,
    full_path: Option<String>,
    app: Option<String>,
    hash: Option<String>,
    md5: Option<String>,
    file_id: Option<u32>,
    file_key: Option<String>,
    audio_format: Option<u32>,
    chat_type: Option<u32>,
    auto_trans_flags: Vec<String>,
    transcribed_text: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct VideoPart {
    title: String,
    path: Option<String>,
    thumb_path: Option<String>,
    size_bytes: Option<u32>,
    md5: Option<String>,
    thumb_size: Option<(u32, u32)>,
    thumb_file_size: Option<u32>,
    source: Option<String>,
    file_time: Option<u32>,
    file_format: Option<u32>,
    video_attr: Option<u32>,
    reserve_hints: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SharePart {
    brief: Option<String>,
    title: Option<String>,
    summary: Option<String>,
    source_name: Option<String>,
    url: Option<String>,
    app_hint: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ForwardRecordPart {
    title: Option<String>,
    brief: Option<String>,
    count: Option<u32>,
    items: Vec<ForwardItem>,
    unresolved_blob_meta: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ForwardItem {
    sender: Option<String>,
    kind: ForwardItemKind,
    summary: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum ForwardItemKind {
    #[default]
    Unknown,
    Text,
    File,
    Image,
    Voice,
    Video,
    Share,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TransferPart {
    text: String,
    file_name: Option<String>,
    app_hint: Option<String>,
}

impl RawData {
    pub fn new(time: i64, rand: i64, sender_uin: i64, msg_content: Vec<u8>, info: Vec<u8>) -> Self {
        Self {
            time,
            rand,
            sender_uin,
            msg_content,
            info,
        }
    }

    pub fn decode(&self) -> Result<String> {
        Ok(self.decode_structured()?.render())
    }

    pub fn decode_compact(&self) -> Result<String> {
        Ok(decode_compact_with_context(&self.msg_content, &self.info)?.text)
    }

    pub fn trace(&self) -> Result<MessageTrace> {
        let info_map = InfoMap::parse(&self.info);
        let info_entries = parse_info_entries_trace(&self.info);
        let info_fallback = info_map.as_ref().and_then(fallback_from_info);
        let (header, payloads) = parse_message_layout(&self.msg_content)?;
        let decoded = self.decode_structured()?;
        let reply_reference = extract_reply_reference_from_payloads(&payloads);
        let outer_payloads = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| trace_outer_payload(index, payload, info_map.as_ref()))
            .collect();

        Ok(MessageTrace {
            header,
            decoded: decoded.render(),
            part_previews: decoded.parts.iter().map(DecodedPart::render).collect(),
            outer_payloads,
            unknown_outer_types: decoded.unknown_outer_types,
            info_entries,
            info_fallback,
            reply_reference,
        })
    }

    fn decode_structured(&self) -> Result<DecodedMessage> {
        let info_map = InfoMap::parse(&self.info);
        let (_header, payloads) = parse_message_layout(&self.msg_content)?;

        let suppress_embedded_images = payloads.iter().any(|payload| {
            matches!(
                payload.payload_type,
                MSG_SHARE | MSG_FILE | MSG_TRANSFER | MSG_TD_27 | MSG_TD_30
            )
        });

        let mut out = Vec::new();
        let mut unknown_visible_types = Vec::new();
        let mut pending_ext_image = None;
        for payload in payloads {
            let decoded =
                match payload.payload_type {
                    MSG_TEXT => parse_text_part(payload.payload).map(DecodedPart::Text),
                    MSG_FACE => parse_face_part(payload.payload).map(DecodedPart::Face),
                    MSG_GROUP_IMAGE if !suppress_embedded_images => {
                        let ext_info = pending_ext_image.take();
                        parse_image_part(payload.payload, "群图片", ext_info.as_ref())
                            .map(DecodedPart::Image)
                    }
                    MSG_PRIVATE_IMAGE if !suppress_embedded_images => {
                        let ext_info = pending_ext_image.take();
                        parse_image_part(payload.payload, "私聊图片", ext_info.as_ref())
                            .map(DecodedPart::Image)
                    }
                    MSG_VOICE => parse_voice_part(payload.payload, info_map.as_ref())
                        .map(DecodedPart::Voice),
                    MSG_EXT_IMAGE => {
                        pending_ext_image = decode_ext_image_payload(payload.payload);
                        None
                    }
                    MSG_EMOJI => parse_emoji_part(payload.payload).map(DecodedPart::Emoji),
                    MSG_TRANSFER => parse_transfer_part(payload.payload, info_map.as_ref())
                        .map(DecodedPart::Transfer),
                    MSG_NICKNAME => {
                        let _ = decode_nickname(payload.payload);
                        None
                    }
                    MSG_SHARE => parse_share_part(payload.payload).map(DecodedPart::Share),
                    MSG_FILE => {
                        parse_file_part(payload.payload, info_map.as_ref()).map(DecodedPart::File)
                    }
                    MSG_VIDEO => parse_video_part(payload.payload, info_map.as_ref())
                        .map(DecodedPart::Video),
                    MSG_TD_27 => parse_td_27_part(payload.payload).map(DecodedPart::Share),
                    MSG_TD_30 => parse_td_30_part(payload.payload, info_map.as_ref())
                        .map(DecodedPart::ForwardRecord),
                    other => {
                        if !METADATA_TYPES.contains(&other) {
                            unknown_visible_types.push(other);
                        }
                        None
                    }
                };

            if let Some(decoded) = decoded {
                if !decoded.render().is_empty() {
                    out.push(decoded);
                }
            }
        }

        if out.is_empty() {
            if let Some(info_fallback) = info_map.as_ref().and_then(fallback_from_info) {
                out.push(DecodedPart::Text(info_fallback));
            }
        }

        Ok(DecodedMessage {
            parts: out,
            unknown_outer_types: unknown_visible_types,
        })
    }

    fn decode_compact_text(&self) -> Result<String> {
        let info_map = InfoMap::parse(&self.info);
        let (_header, payloads) = parse_message_layout(&self.msg_content)?;

        let suppress_embedded_images = payloads.iter().any(|payload| {
            matches!(
                payload.payload_type,
                MSG_SHARE | MSG_FILE | MSG_TRANSFER | MSG_TD_27 | MSG_TD_30
            )
        });

        let mut out = Vec::new();
        let mut unknown_visible_types = Vec::new();
        let mut pending_ext_image = None;
        for payload in payloads {
            let decoded = match payload.payload_type {
                MSG_TEXT => parse_text_part(payload.payload),
                MSG_FACE => parse_face_part(payload.payload),
                MSG_GROUP_IMAGE if !suppress_embedded_images => {
                    let ext_info = pending_ext_image.take();
                    parse_image_part_compact(payload.payload, "群图片", ext_info.as_ref())
                }
                MSG_PRIVATE_IMAGE if !suppress_embedded_images => {
                    let ext_info = pending_ext_image.take();
                    parse_image_part_compact(payload.payload, "私聊图片", ext_info.as_ref())
                }
                MSG_VOICE => parse_voice_part_compact(payload.payload, info_map.as_ref()),
                MSG_EXT_IMAGE => {
                    pending_ext_image = decode_ext_image_payload(payload.payload);
                    None
                }
                MSG_EMOJI => parse_emoji_part(payload.payload),
                MSG_TRANSFER => {
                    parse_transfer_part(payload.payload, info_map.as_ref()).map(|part| part.text)
                }
                MSG_NICKNAME => None,
                MSG_SHARE => parse_share_part(payload.payload).map(|part| render_share_part(&part)),
                MSG_FILE => parse_file_part_compact(payload.payload, info_map.as_ref()),
                MSG_VIDEO => parse_video_part_compact(payload.payload, info_map.as_ref()),
                MSG_TD_27 => parse_td_27_part(payload.payload).map(|part| render_share_part(&part)),
                MSG_TD_30 => parse_td_30_part(payload.payload, info_map.as_ref())
                    .map(|part| render_forward_record(&part)),
                other => {
                    if !METADATA_TYPES.contains(&other) {
                        unknown_visible_types.push(other);
                    }
                    None
                }
            };

            if let Some(decoded) = decoded {
                if !decoded.is_empty() {
                    out.push(decoded);
                }
            }
        }

        if out.is_empty() {
            if let Some(info_fallback) = info_map.as_ref().and_then(fallback_from_info) {
                out.push(info_fallback);
            }
        }

        let merged = out.join("");
        if !merged.is_empty() {
            return Ok(merged);
        }
        if !unknown_visible_types.is_empty() {
            return Ok(format!("[未解析消息:{:?}]", unknown_visible_types));
        }
        Ok(String::new())
    }
}

pub fn decode_compact_with_context(
    msg_content: &[u8],
    info: &[u8],
) -> Result<CompactDecodeContext> {
    let start_ptr = skip_message_header_fast(msg_content)?;
    if !contains_image_payload(msg_content, start_ptr)? {
        return decode_compact_with_context_no_image(msg_content, info, start_ptr);
    }

    let (_header, payloads) = parse_message_layout(msg_content)?;

    let suppress_embedded_images = payloads.iter().any(|payload| {
        matches!(
            payload.payload_type,
            MSG_SHARE | MSG_FILE | MSG_TRANSFER | MSG_TD_27 | MSG_TD_30
        )
    });

    let mut out = String::with_capacity(msg_content.len().min(256));
    let mut unknown_visible_types = Vec::new();
    let mut pending_ext_image = None;
    let mut reply_reference = None;
    let mut info_map = LazyInfoMap::new(info);

    for payload in payloads {
        if reply_reference.is_none() && payload.payload_type == 0x19 {
            reply_reference = extract_reply_reference_from_metadata_payload(payload.payload);
        }

        let decoded = match payload.payload_type {
            MSG_TEXT => parse_text_part(payload.payload),
            MSG_FACE => parse_face_part(payload.payload),
            MSG_GROUP_IMAGE if !suppress_embedded_images => {
                let ext_info = pending_ext_image.take();
                parse_image_part_compact(payload.payload, "群图片", ext_info.as_ref())
            }
            MSG_PRIVATE_IMAGE if !suppress_embedded_images => {
                let ext_info = pending_ext_image.take();
                parse_image_part_compact(payload.payload, "私聊图片", ext_info.as_ref())
            }
            MSG_VOICE => parse_voice_part_compact(payload.payload, info_map.get()),
            MSG_EXT_IMAGE => {
                pending_ext_image = decode_ext_image_payload(payload.payload);
                None
            }
            MSG_EMOJI => parse_emoji_part(payload.payload),
            MSG_TRANSFER => {
                parse_transfer_part(payload.payload, info_map.get()).map(|part| part.text)
            }
            MSG_NICKNAME => None,
            MSG_SHARE => parse_share_part(payload.payload).map(|part| render_share_part(&part)),
            MSG_FILE => parse_file_part_compact(payload.payload, info_map.get()),
            MSG_VIDEO => parse_video_part_compact(payload.payload, info_map.get()),
            MSG_TD_27 => {
                parse_td_27_part(payload.payload).map(|part| render_share_part(&part))
            }
            MSG_TD_30 => parse_td_30_part(payload.payload, info_map.get())
                .map(|part| render_forward_record(&part)),
            other => {
                if !METADATA_TYPES.contains(&other) {
                    unknown_visible_types.push(other);
                }
                None
            }
        };

        if let Some(decoded) = decoded.filter(|decoded| !decoded.is_empty()) {
            out.push_str(&decoded);
        }
    }

    if out.is_empty() {
        if let Some(info_fallback) = info_map.get().and_then(fallback_from_info) {
            out = info_fallback;
        }
    }

    let text = if !out.is_empty() {
        normalize_compact_decoded_text(out)
    } else if !unknown_visible_types.is_empty() {
        format!("[未解析消息:{:?}]", unknown_visible_types)
    } else {
        String::new()
    };

    let sender_show_name = info_map
        .parsed()
        .and_then(sender_show_name_from_info_map)
        .or_else(|| extract_sender_show_name(info));

    Ok(CompactDecodeContext {
        text,
        reply_reference,
        sender_show_name,
    })
}

fn decode_compact_with_context_no_image(
    msg_content: &[u8],
    info: &[u8],
    start_ptr: usize,
) -> Result<CompactDecodeContext> {
    let mut out = String::with_capacity(msg_content.len().min(256));
    let mut unknown_visible_types = Vec::new();
    let mut reply_reference = None;
    let mut info_map = LazyInfoMap::new(info);

    let mut ptr = start_ptr;
    while ptr < msg_content.len() {
        let payload_type = match read_u8(msg_content, &mut ptr) {
            Ok(value) => value,
            Err(_) => break,
        };
        let payload_len = match read_u16_le(msg_content, &mut ptr) {
            Ok(value) => value as usize,
            Err(_) => break,
        };
        let payload = match read_slice(msg_content, &mut ptr, payload_len) {
            Ok(slice) => slice,
            Err(_) => break,
        };

        if reply_reference.is_none() && payload_type == 0x19 {
            reply_reference = extract_reply_reference_from_metadata_payload(payload);
        }

        let decoded = match payload_type {
            MSG_TEXT => parse_text_part(payload),
            MSG_FACE => parse_face_part(payload),
            MSG_VOICE => parse_voice_part_compact(payload, info_map.get()),
            MSG_EMOJI => parse_emoji_part(payload),
            MSG_TRANSFER => parse_transfer_part(payload, info_map.get()).map(|part| part.text),
            MSG_NICKNAME => None,
            MSG_SHARE => parse_share_part(payload).map(|part| render_share_part(&part)),
            MSG_FILE => parse_file_part_compact(payload, info_map.get()),
            MSG_VIDEO => parse_video_part_compact(payload, info_map.get()),
            MSG_TD_27 => parse_td_27_part(payload).map(|part| render_share_part(&part)),
            MSG_TD_30 => {
                parse_td_30_part(payload, info_map.get()).map(|part| render_forward_record(&part))
            }
            other => {
                if !METADATA_TYPES.contains(&other) {
                    unknown_visible_types.push(other);
                }
                None
            }
        };

        if let Some(decoded) = decoded.filter(|decoded| !decoded.is_empty()) {
            out.push_str(&decoded);
        }
    }

    if out.is_empty() {
        if let Some(info_fallback) = info_map.get().and_then(fallback_from_info) {
            out = info_fallback;
        }
    }

    let text = if !out.is_empty() {
        normalize_compact_decoded_text(out)
    } else if !unknown_visible_types.is_empty() {
        format!("[未解析消息:{:?}]", unknown_visible_types)
    } else {
        String::new()
    };

    let sender_show_name = info_map
        .parsed()
        .and_then(sender_show_name_from_info_map)
        .or_else(|| extract_sender_show_name(info));

    Ok(CompactDecodeContext {
        text,
        reply_reference,
        sender_show_name,
    })
}

pub fn extract_sender_show_name(info: &[u8]) -> Option<String> {
    let (value_enc, value_len) = find_info_value_by_encoded_key(info, sender_show_name_key_bytes())?;
    let decoded = decode_info_encoded_bytes(value_enc, value_len as u8);
    decode_textish_raw(&decoded).or_else(|| decode_info_text_value("strSenderShowName", &decoded))
}

pub fn register_sqlite_functions(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "qq_sender_show_name",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let info = ctx
                .get_raw(0)
                .as_blob_or_null()
                .map_err(|err| SqlError::UserFunctionError(err.into()))?;
            Ok(info.and_then(extract_sender_show_name))
        },
    )?;
    Ok(())
}

pub fn extract_msg_seq(info: &[u8]) -> Option<u32> {
    find_info_u32_by_encoded_key(info, msg_seq_key_bytes())
        .or_else(|| find_info_u32_by_encoded_key(info, begin_seq_key_bytes()))
        .or_else(|| find_info_u32_by_encoded_key(info, end_seq_key_bytes()))
}

pub fn compact_decode_requires_full_info(msg_content: &[u8]) -> bool {
    let Ok(mut ptr) = skip_message_header_fast(msg_content) else {
        return true;
    };

    let mut has_visible_without_info = false;
    let mut has_reply_reference = false;

    while ptr < msg_content.len() {
        let payload_type = match read_u8(msg_content, &mut ptr) {
            Ok(value) => value,
            Err(_) => return true,
        };
        let payload_len = match read_u16_le(msg_content, &mut ptr) {
            Ok(value) => value as usize,
            Err(_) => return true,
        };
        if read_slice(msg_content, &mut ptr, payload_len).is_err() {
            return true;
        }

        match payload_type {
            MSG_VOICE | MSG_TRANSFER | MSG_FILE | MSG_VIDEO | MSG_TD_30 => return true,
            MSG_TEXT | MSG_FACE | MSG_GROUP_IMAGE | MSG_PRIVATE_IMAGE | MSG_EMOJI | MSG_SHARE
            | MSG_TD_27 => {
                has_visible_without_info = true;
            }
            0x19 => has_reply_reference = true,
            other => {
                if !METADATA_TYPES.contains(&other)
                    && other != MSG_EXT_IMAGE
                    && other != MSG_NICKNAME
                {
                    has_visible_without_info = true;
                }
            }
        }
    }

    !(has_visible_without_info || has_reply_reference)
}

pub fn extract_reply_reference(msg_content: &[u8]) -> Option<ReplyReference> {
    let mut ptr = skip_message_header_fast(msg_content).ok()?;

    while ptr < msg_content.len() {
        let payload_type = read_u8(msg_content, &mut ptr).ok()?;
        let payload_len = read_u16_le(msg_content, &mut ptr).ok()? as usize;
        let payload = read_slice(msg_content, &mut ptr, payload_len).ok()?;
        if payload_type == 0x19 {
            if let Some(reply) = extract_reply_reference_from_metadata_payload(payload) {
                return Some(reply);
            }
        }
    }

    None
}

fn skip_message_header_fast(data: &[u8]) -> Result<usize> {
    if data.len() < 26 {
        bail!("msg_content too short: {}", data.len());
    }

    let mut ptr = 8;
    let _ = read_u32_le(data, &mut ptr)?;
    let _ = read_u32_le(data, &mut ptr)?;
    let _ = read_u32_le(data, &mut ptr)?;
    let _ = read_u8(data, &mut ptr)?;
    let _ = read_u8(data, &mut ptr)?;
    let _ = read_u8(data, &mut ptr)?;
    let _ = read_u8(data, &mut ptr)?;
    let font_name_len = read_u16_le(data, &mut ptr)? as usize;
    let _ = read_slice(data, &mut ptr, font_name_len)?;
    let _ = read_slice(data, &mut ptr, 2)?;
    Ok(ptr)
}

fn contains_image_payload(msg_content: &[u8], start_ptr: usize) -> Result<bool> {
    let mut ptr = start_ptr;
    while ptr < msg_content.len() {
        let payload_type = match read_u8(msg_content, &mut ptr) {
            Ok(value) => value,
            Err(_) => break,
        };
        let payload_len = read_u16_le(msg_content, &mut ptr)? as usize;
        let _ = read_slice(msg_content, &mut ptr, payload_len)?;
        if matches!(
            payload_type,
            MSG_GROUP_IMAGE | MSG_PRIVATE_IMAGE | MSG_EXT_IMAGE
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn extract_reply_reference_from_payloads(
    payloads: &[ParsedOuterPayload<'_>],
) -> Option<ReplyReference> {
    for payload in payloads {
        if payload.payload_type != 0x19 {
            continue;
        }
        if let Some(reply) = extract_reply_reference_from_metadata_payload(payload.payload) {
            return Some(reply);
        }
    }
    None
}

fn extract_reply_reference_from_metadata_payload(payload: &[u8]) -> Option<ReplyReference> {
    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if inner_type == 0x01 {
            if let Some(reply) = extract_reply_reference_from_proto(data) {
                return Some(reply);
            }
        }
        ptr += len;
    }
    None
}

fn extract_reply_reference_from_proto(data: &[u8]) -> Option<ReplyReference> {
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        if field == 45 {
            if let ProtoValue::Len(reply_body) = value {
                return parse_reply_reference_body(reply_body);
            }
        }
    }
    None
}

fn parse_reply_reference_body(data: &[u8]) -> Option<ReplyReference> {
    let mut out = ReplyReference::default();
    let mut ptr = 0;

    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        match (field, value) {
            (1, ProtoValue::Varint(value)) => {
                out.target_seq = u32::try_from(value).ok();
            }
            (2, ProtoValue::Varint(value)) => {
                out.target_sender_uin = i64::try_from(value).ok();
            }
            (3, ProtoValue::Varint(value)) => {
                out.target_time = i64::try_from(value).ok();
            }
            (5, ProtoValue::Len(value)) => {
                let text = parse_reply_preview_item(value);
                if !text.is_empty() {
                    out.preview.get_or_insert_with(String::new).push_str(&text);
                }
            }
            (8, ProtoValue::Len(value)) => {
                if let Some(rand) = parse_reply_random(value) {
                    out.target_rand = Some(rand);
                }
            }
            _ => {}
        }
    }

    if out.preview.as_deref().is_some_and(|text| text.is_empty()) {
        out.preview = None;
    }

    (out.target_seq.is_some()
        || out.target_sender_uin.is_some()
        || out.target_time.is_some()
        || out.target_rand.is_some()
        || out.preview.is_some())
    .then_some(out)
}

fn parse_reply_preview_item(data: &[u8]) -> String {
    let mut ptr = 0;
    let mut out = String::new();
    let mut has_face = false;
    let mut has_image = false;

    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        match (field, value) {
            (1, ProtoValue::Len(value)) => {
                if let Some(text) = parse_reply_preview_text(value) {
                    out.push_str(&text);
                }
            }
            (2 | 6 | 34, _) => has_face = true,
            (4 | 8, _) => has_image = true,
            _ => {}
        }
    }

    if !out.is_empty() {
        return out;
    }
    if has_image {
        return "[图片]".to_string();
    }
    if has_face {
        return "[表情]".to_string();
    }
    String::new()
}

fn parse_reply_preview_text(data: &[u8]) -> Option<String> {
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        if field != 1 {
            continue;
        }
        if let ProtoValue::Len(bytes) = value {
            let text = String::from_utf8_lossy(bytes).to_string();
            let text = text.trim_matches('\0').to_string();
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn parse_reply_random(data: &[u8]) -> Option<i64> {
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        if field != 3 {
            continue;
        }
        if let ProtoValue::Varint(value) = value {
            return Some((value & 0xffff_ffff) as i64);
        }
    }
    None
}

fn parse_message_layout<'a>(
    data: &'a [u8],
) -> Result<(MessageHeaderTrace, Vec<ParsedOuterPayload<'a>>)> {
    if data.len() < 26 {
        bail!("msg_content too short: {}", data.len());
    }

    let mut ptr = 8; // 跳过 MSG 头
    let header = MessageHeaderTrace {
        time: read_u32_le(data, &mut ptr)?,
        rand: read_u32_le(data, &mut ptr)?,
        color: read_u32_le(data, &mut ptr)?,
        font_size: read_u8(data, &mut ptr)?,
        font_style: read_u8(data, &mut ptr)?,
        charset: read_u8(data, &mut ptr)?,
        font_family: read_u8(data, &mut ptr)?,
        font_name: {
            let font_name_len = read_u16_le(data, &mut ptr)? as usize;
            decode_utf16le(read_slice(data, &mut ptr, font_name_len)?)
        },
    };
    let _ = read_slice(data, &mut ptr, 2)?; // 固定尾随字节

    let mut payloads = Vec::new();
    while ptr < data.len() {
        let offset = ptr;
        let payload_type = match read_u8(data, &mut ptr) {
            Ok(v) => v,
            Err(_) => break,
        };
        let payload_len = read_u16_le(data, &mut ptr)? as usize;
        let payload = read_slice(data, &mut ptr, payload_len)?;
        payloads.push(ParsedOuterPayload {
            offset,
            payload_type,
            payload,
        });
    }

    Ok((header, payloads))
}

fn trace_outer_payload(
    index: usize,
    payload: &ParsedOuterPayload<'_>,
    info: Option<&InfoMap>,
) -> OuterPayloadTrace {
    let preview = preview_outer_payload(payload.payload_type, payload.payload, info);
    let hints = if preview.is_none() {
        binary_payload_hints(payload.payload)
    } else {
        Vec::new()
    };
    OuterPayloadTrace {
        index,
        offset: payload.offset,
        payload_type: payload.payload_type,
        label: payload_type_label(payload.payload_type).to_string(),
        length: payload.payload.len(),
        preview,
        hints,
        raw_hex_prefix: hex_string_prefix(payload.payload, 96),
    }
}

fn parse_info_entries_trace(data: &[u8]) -> Vec<InfoEntryTrace> {
    if data.len() < 9 || !data.starts_with(b"TD") {
        return Vec::new();
    }

    let mut ptr = 6;
    let mut entries = Vec::new();
    while ptr + 3 <= data.len() {
        let tag = data[ptr];
        ptr += 1;

        let key_len = match read_u16_le(data, &mut ptr) {
            Ok(value) => value as usize,
            Err(_) => break,
        };
        let key_enc = match read_slice(data, &mut ptr, key_len) {
            Ok(slice) => slice,
            Err(_) => break,
        };

        let key = decode_info_encoded_bytes(key_enc, key_len as u8);
        let key = decode_utf16le(&key).trim_matches('\0').to_string();
        if key.is_empty() {
            break;
        }

        let value_len = match read_u32_le(data, &mut ptr) {
            Ok(value) => value as usize,
            Err(_) => break,
        };
        let value_enc = match read_slice(data, &mut ptr, value_len) {
            Ok(slice) => slice,
            Err(_) => break,
        };
        let value = if info_value_is_raw_numeric(&key, value_len) {
            value_enc.to_vec()
        } else {
            decode_info_encoded_bytes(value_enc, value_len as u8)
        };
        let text = if info_value_is_raw_numeric(&key, value_len) {
            None
        } else {
            decode_info_text_value(&key, &value)
        };
        let numeric_u32 = if value.len() == 4 {
            Some(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
        } else {
            None
        };
        let nested_keys = parse_nested_td_payload(&value)
            .map(|info| {
                let mut keys = info.entries.keys().cloned().collect::<Vec<_>>();
                keys.sort();
                keys
            })
            .unwrap_or_default();
        let text_hints = select_text_hints(extract_text_candidates(&value), 3);

        entries.push(InfoEntryTrace {
            index: entries.len(),
            tag,
            key,
            value_len,
            text,
            numeric_u32,
            nested_keys,
            text_hints,
            raw_hex_prefix: hex_string_prefix(&value, 96),
        });
    }

    entries
}

fn preview_outer_payload(
    payload_type: u8,
    payload: &[u8],
    info: Option<&InfoMap>,
) -> Option<String> {
    match payload_type {
        MSG_TEXT => parse_text_part(payload),
        MSG_FACE => parse_face_part(payload),
        MSG_GROUP_IMAGE => {
            parse_image_part(payload, "群图片", None).map(|part| render_image_part(&part))
        }
        MSG_PRIVATE_IMAGE => {
            parse_image_part(payload, "私聊图片", None).map(|part| render_image_part(&part))
        }
        MSG_VOICE => parse_voice_part(payload, info).map(|part| render_voice_part(&part)),
        MSG_EXT_IMAGE => decode_ext_image_payload(payload).map(|part| {
            let mut out = "[扩展图片]".to_string();
            if let Some(name) = part.name.as_deref() {
                out.push(' ');
                out.push_str(name);
            }
            if let Some(size) = part.size_text() {
                out.push_str(" {size=");
                out.push_str(&size);
                out.push('}');
            }
            out
        }),
        MSG_EMOJI => parse_emoji_part(payload),
        MSG_TRANSFER => parse_transfer_part(payload, info).map(|part| part.text),
        MSG_NICKNAME => decode_nickname(payload),
        MSG_SHARE => parse_share_part(payload).map(|part| render_share_part(&part)),
        MSG_FILE => parse_file_part(payload, info).map(|part| format_file_info(&part)),
        MSG_VIDEO => parse_video_part(payload, info).map(|part| render_video_part(&part)),
        MSG_TD_27 => parse_td_27_part(payload).map(|part| render_share_part(&part)),
        MSG_TD_30 => parse_td_30_part(payload, info).map(|part| render_forward_record(&part)),
        0x19 => extract_reply_reference_from_metadata_payload(payload).map(render_reply_reference),
        _ => None,
    }
}

fn render_reply_reference(reply: ReplyReference) -> String {
    let mut out = String::from("[回复");
    if let Some(seq) = reply.target_seq {
        out.push_str(&format!(" seq={seq}"));
    }
    if let Some(sender_uin) = reply.target_sender_uin {
        out.push_str(&format!(", uin={sender_uin}"));
    }
    if let Some(time) = reply.target_time {
        out.push_str(&format!(", time={time}"));
    }
    if let Some(rand) = reply.target_rand {
        out.push_str(&format!(", rand={rand}"));
    }
    out.push(']');
    if let Some(preview) = reply.preview {
        if !preview.trim().is_empty() {
            out.push(' ');
            out.push_str(preview.trim());
        }
    }
    out
}

fn payload_type_label(payload_type: u8) -> &'static str {
    match payload_type {
        MSG_TEXT => "text",
        MSG_FACE => "face",
        MSG_GROUP_IMAGE => "group_image",
        MSG_PRIVATE_IMAGE => "private_image",
        MSG_VOICE => "voice",
        MSG_EXT_IMAGE => "ext_image",
        MSG_EMOJI => "emoji",
        MSG_TRANSFER => "transfer",
        MSG_NICKNAME => "nickname",
        MSG_SHARE => "share",
        MSG_FILE => "file",
        MSG_VIDEO => "video",
        MSG_TD_27 => "td_27_share",
        MSG_TD_30 => "td_30_forward",
        0x00 => "metadata_0x00",
        0x0e => "metadata_0x0e",
        0x19 => "metadata_0x19",
        _ => "unknown",
    }
}

fn hex_string_prefix(data: &[u8], limit: usize) -> String {
    hex_string(&data[..data.len().min(limit)])
}

impl DecodedMessage {
    pub fn render(&self) -> String {
        let merged = self
            .parts
            .iter()
            .map(DecodedPart::render)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("");

        if !merged.is_empty() {
            return merged;
        }
        if !self.unknown_outer_types.is_empty() {
            return format!("[未解析消息:{:?}]", self.unknown_outer_types);
        }
        String::new()
    }

    pub fn render_compact(&self) -> String {
        let merged = self
            .parts
            .iter()
            .map(DecodedPart::render_compact)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join("");

        if !merged.is_empty() {
            return merged;
        }
        if !self.unknown_outer_types.is_empty() {
            return format!("[未解析消息:{:?}]", self.unknown_outer_types);
        }
        String::new()
    }
}

impl DecodedPart {
    pub fn render(&self) -> String {
        match self {
            DecodedPart::Text(text) | DecodedPart::Face(text) | DecodedPart::Emoji(text) => {
                text.clone()
            }
            DecodedPart::Image(image) => render_image_part(image),
            DecodedPart::Voice(voice) => render_voice_part(voice),
            DecodedPart::Video(video) => render_video_part(video),
            DecodedPart::File(file) => format_file_info(file),
            DecodedPart::Share(share) => render_share_part(share),
            DecodedPart::ForwardRecord(record) => render_forward_record(record),
            DecodedPart::Transfer(transfer) => transfer.text.clone(),
        }
    }

    pub fn render_compact(&self) -> String {
        match self {
            DecodedPart::Text(text) | DecodedPart::Face(text) | DecodedPart::Emoji(text) => {
                text.clone()
            }
            DecodedPart::Image(image) => render_image_part_compact(image),
            DecodedPart::Voice(voice) => render_voice_part_compact(voice),
            DecodedPart::Video(video) => render_video_part_compact(video),
            DecodedPart::File(file) => format_file_info_compact(file),
            DecodedPart::Share(share) => render_share_part(share),
            DecodedPart::ForwardRecord(record) => render_forward_record(record),
            DecodedPart::Transfer(transfer) => transfer.text.clone(),
        }
    }
}

impl ExtImageInfo {
    fn size_text(&self) -> Option<String> {
        match (self.width, self.height) {
            (Some(width), Some(height)) => Some(format!("{width}x{height}")),
            (Some(width), None) => Some(width.to_string()),
            (None, Some(height)) => Some(height.to_string()),
            (None, None) => None,
        }
    }
}

impl InfoMap {
    fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 9 || !data.starts_with(b"TD") {
            return None;
        }

        let mut ptr = 6; // 跳过 TD 头
        let mut entries = HashMap::new();

        while ptr + 3 <= data.len() {
            let _tag = data[ptr];
            ptr += 1;

            let key_len = u16::from_le_bytes([data[ptr], data[ptr + 1]]) as usize;
            ptr += 2;
            let key_enc = match read_slice(data, &mut ptr, key_len) {
                Ok(slice) => slice,
                Err(_) => break,
            };

            let key = decode_info_encoded_bytes(key_enc, key_len as u8);
            let key = decode_utf16le(&key).trim_matches('\0').to_string();
            if key.is_empty() {
                break;
            }

            let value_len = match read_u32_le(data, &mut ptr) {
                Ok(len) => len as usize,
                Err(_) => break,
            };
            let value_enc = match read_slice(data, &mut ptr, value_len) {
                Ok(slice) => slice,
                Err(_) => break,
            };

            let value = if info_value_is_raw_numeric(&key, value_len) {
                InfoValue {
                    raw: value_enc.to_vec(),
                    text: None,
                }
            } else {
                let decoded = decode_info_encoded_bytes(value_enc, value_len as u8);
                InfoValue {
                    text: decode_info_text_value(&key, &decoded),
                    raw: decoded,
                }
            };

            entries.insert(key, value);
        }

        if entries.is_empty() {
            None
        } else {
            Some(Self { entries })
        }
    }

    fn text(&self, key: &str) -> Option<&str> {
        self.entries.get(key)?.text.as_deref()
    }

    fn raw(&self, key: &str) -> Option<&[u8]> {
        Some(self.entries.get(key)?.raw.as_slice())
    }

    fn u32(&self, key: &str) -> Option<u32> {
        let raw = self.entries.get(key)?.raw.as_slice();
        if raw.len() == 4 {
            Some(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
        } else {
            None
        }
    }

    fn textish(&self, key: &str) -> Option<String> {
        self.raw(key)
            .and_then(decode_textish_raw)
            .or_else(|| self.text(key).map(str::to_string))
    }

    fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }
}

impl<'a> LazyInfoMap<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, parsed: None }
    }

    fn get(&mut self) -> Option<&InfoMap> {
        if self.parsed.is_none() {
            self.parsed = Some(InfoMap::parse(self.raw));
        }
        self.parsed.as_ref().and_then(|parsed| parsed.as_ref())
    }

    fn parsed(&self) -> Option<&InfoMap> {
        self.parsed.as_ref().and_then(|parsed| parsed.as_ref())
    }
}

fn sender_show_name_from_info_map(info: &InfoMap) -> Option<String> {
    info.textish("strSenderShowName")
        .or_else(|| info.text("strSenderShowName").map(str::to_string))
}

fn extract_info_textish_field(info: &[u8], target_key: &str) -> Option<String> {
    let (key, value_enc, _value_len) = extract_info_field_raw(info, |key| key == target_key)?;
    let decoded = decode_info_encoded_bytes(value_enc, value_enc.len() as u8);
    decode_textish_raw(&decoded).or_else(|| decode_info_text_value(&key, &decoded))
}

fn sender_show_name_key_bytes() -> &'static [u8] {
    static KEY: OnceLock<Vec<u8>> = OnceLock::new();
    KEY.get_or_init(|| encode_info_key_bytes("strSenderShowName"))
        .as_slice()
}

fn msg_seq_key_bytes() -> &'static [u8] {
    static KEY: OnceLock<Vec<u8>> = OnceLock::new();
    KEY.get_or_init(|| encode_info_key_bytes("dwMsgSeq"))
        .as_slice()
}

fn begin_seq_key_bytes() -> &'static [u8] {
    static KEY: OnceLock<Vec<u8>> = OnceLock::new();
    KEY.get_or_init(|| encode_info_key_bytes("dwBeginSeq"))
        .as_slice()
}

fn end_seq_key_bytes() -> &'static [u8] {
    static KEY: OnceLock<Vec<u8>> = OnceLock::new();
    KEY.get_or_init(|| encode_info_key_bytes("dwEndSeq"))
        .as_slice()
}

fn encode_info_key_bytes(text: &str) -> Vec<u8> {
    let raw = text
        .encode_utf16()
        .flat_map(|word| word.to_le_bytes())
        .collect::<Vec<_>>();
    let key = raw.len() as u8;
    raw.into_iter()
        .map(|byte| ((!byte) & 0xff) ^ key)
        .collect()
}

fn find_info_value_by_encoded_key<'a>(
    data: &'a [u8],
    encoded_key: &[u8],
) -> Option<(&'a [u8], usize)> {
    if data.len() < 9 || !data.starts_with(b"TD") {
        return None;
    }

    let mut ptr = 6;
    while ptr + 3 <= data.len() {
        ptr += 1;

        let key_len = read_u16_le(data, &mut ptr).ok()? as usize;
        let key_enc = read_slice(data, &mut ptr, key_len).ok()?;
        let value_len = read_u32_le(data, &mut ptr).ok()? as usize;
        let value_enc = read_slice(data, &mut ptr, value_len).ok()?;
        if key_enc == encoded_key {
            return Some((value_enc, value_len));
        }
    }

    None
}

fn find_info_u32_by_encoded_key(data: &[u8], encoded_key: &[u8]) -> Option<u32> {
    let (value_enc, value_len) = find_info_value_by_encoded_key(data, encoded_key)?;
    if value_len != 4 || value_enc.len() != 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        value_enc[0],
        value_enc[1],
        value_enc[2],
        value_enc[3],
    ]))
}

fn extract_info_u32_field(info: &[u8], target_key: &str) -> Option<u32> {
    let (_key, value_enc, value_len) = extract_info_field_raw(info, |key| key == target_key)?;
    if value_len != 4 || value_enc.len() != 4 {
        return None;
    }
    Some(u32::from_le_bytes([
        value_enc[0],
        value_enc[1],
        value_enc[2],
        value_enc[3],
    ]))
}

fn extract_info_field_raw<'a, F>(
    data: &'a [u8],
    mut matcher: F,
) -> Option<(String, &'a [u8], usize)>
where
    F: FnMut(&str) -> bool,
{
    if data.len() < 9 || !data.starts_with(b"TD") {
        return None;
    }

    let mut ptr = 6;
    while ptr + 3 <= data.len() {
        ptr += 1;

        let key_len = read_u16_le(data, &mut ptr).ok()? as usize;
        let key_enc = read_slice(data, &mut ptr, key_len).ok()?;
        let key = decode_utf16le(&decode_info_encoded_bytes(key_enc, key_len as u8))
            .trim_matches('\0')
            .to_string();
        if key.is_empty() {
            break;
        }

        let value_len = read_u32_le(data, &mut ptr).ok()? as usize;
        let value_enc = read_slice(data, &mut ptr, value_len).ok()?;
        if matcher(&key) {
            return Some((key, value_enc, value_len));
        }
    }

    None
}

fn decode_info_encoded_bytes(data: &[u8], key: u8) -> Vec<u8> {
    data.iter().map(|byte| ((!*byte) & 0xff) ^ key).collect()
}

fn info_value_is_raw_numeric(key: &str, len: usize) -> bool {
    len <= 8
        && !key.starts_with("str")
        && !key.starts_with("MSGBOX_")
        && !key.starts_with("bs")
        && !key.starts_with("guid")
        && !key.starts_with("arr")
        && !key.starts_with("buf")
}

fn decode_info_text_value(key: &str, data: &[u8]) -> Option<String> {
    if data.is_empty() {
        return None;
    }

    if key.to_ascii_lowercase().contains("md5") {
        return None;
    }

    let mut candidates = Vec::new();
    if let Ok(text) = std::str::from_utf8(data) {
        let text = text.trim_matches('\0').trim().to_string();
        if !text.is_empty() {
            candidates.push(text);
        }
    }
    if data.len() % 2 == 0 {
        let text = decode_utf16le(data).trim_matches('\0').trim().to_string();
        if !text.is_empty() {
            candidates.push(text);
        }
    }

    candidates
        .into_iter()
        .map(|text| (text_hint_score(&text), text))
        .filter(|(score, _)| *score >= 12)
        .max_by(|left, right| left.0.cmp(&right.0))
        .map(|(_, text)| text)
}

fn fallback_from_info(info: &InfoMap) -> Option<String> {
    if let Some(path) = info.text("strFileSelect") {
        let file_name = file_name_from_path(path);
        if !file_name.is_empty() {
            return Some(format!("成功接收离线文件“{file_name}”"));
        }
    }
    if let Some(preview) = info.text("MSGBOX_bsPreviewMsgText") {
        return Some(preview.to_string());
    }
    None
}

fn file_name_from_path(path: &str) -> String {
    path.rsplit(['\\', '/'])
        .next()
        .unwrap_or(path)
        .trim()
        .to_string()
}

fn parse_nested_td_payload(payload: &[u8]) -> Option<InfoMap> {
    let mut merged = InfoMap::default();
    let mut found = false;

    if let Some(map) = InfoMap::parse(payload) {
        merged.entries.extend(map.entries);
        found = true;
    }

    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if matches!(inner_type, 0x00 | 0x01 | 0x02) {
            if let Some(map) = InfoMap::parse(data) {
                merged.entries.extend(map.entries);
                found = true;
            }
        }
        ptr += len;
    }

    found.then_some(merged)
}

fn parse_text_part(payload: &[u8]) -> Option<String> {
    let text = decode_text_payload(payload);
    (!text.is_empty()).then_some(text)
}

fn normalize_compact_decoded_text(text: String) -> String {
    let trimmed = text.trim_matches(|ch| matches!(ch, '\r' | '\n' | '\0'));
    if trimmed.is_empty() {
        return String::new();
    }

    let normalized = if trimmed.contains('@') {
        collapse_leading_duplicate_mentions(&rewrite_inline_mentions(trimmed))
    } else {
        trimmed.to_string()
    };

    normalized
}

fn rewrite_inline_mentions(text: &str) -> String {
    mention_raw_regex()
        .replace_all(text, "@$1($2)")
        .into_owned()
}

fn collapse_leading_duplicate_mentions(text: &str) -> String {
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

fn parse_face_part(payload: &[u8]) -> Option<String> {
    let text = decode_face_payload(payload);
    (!text.is_empty()).then_some(text)
}

fn parse_emoji_part(payload: &[u8]) -> Option<String> {
    let text = decode_emoji_payload(payload);
    (!text.is_empty()).then_some(text)
}

fn parse_image_part(
    payload: &[u8],
    label: &str,
    ext_info: Option<&ExtImageInfo>,
) -> Option<ImagePart> {
    collect_image_part(payload, label, ext_info)
}

fn render_image_part(image: &ImagePart) -> String {
    let mut extra = Vec::new();
    if let Some(name) = image.name.as_deref() {
        let normalized_name = normalize_bracketed_name(name);
        if normalized_name != image.label {
            extra.push(format!("name={name}"));
        }
    }
    if let Some(size_text) = image.size_text.as_deref() {
        extra.push(format!("size={size_text}"));
    }
    if let Some(path) = image.path.as_deref().filter(|path| !path.is_empty()) {
        extra.push(format!("path={path}"));
    }
    if let Some(hash) = image.hash.as_deref().filter(|hash| !hash.is_empty()) {
        extra.push(format!("hash={hash}"));
    }

    if extra.is_empty() {
        format!("[{}]", image.label)
    } else {
        format!("[{},{}]", image.label, extra.join(","))
    }
}

fn render_image_part_compact(image: &ImagePart) -> String {
    let label = format!("[{}]", image.label);
    if let Some(path) = image.path.as_deref().filter(|path| !path.is_empty()) {
        return format!("{label} path={path}");
    }
    if let Some(name) = image.name.as_deref().filter(|name| !name.is_empty()) {
        return format!("{label} name={name}");
    }
    label
}

fn parse_image_part_compact(
    payload: &[u8],
    label: &str,
    ext_info: Option<&ExtImageInfo>,
) -> Option<String> {
    collect_image_part_compact(payload, label, ext_info)
}

fn parse_voice_part(payload: &[u8], info: Option<&InfoMap>) -> Option<VoicePart> {
    collect_voice_part(payload, info)
}

fn render_voice_part(voice: &VoicePart) -> String {
    let mut extras = Vec::new();
    if let Some(path) = voice.path.as_deref().filter(|path| !path.is_empty()) {
        extras.push(format!("path={path}"));
    }
    if let Some(full_path) = voice
        .full_path
        .as_deref()
        .filter(|full_path| !full_path.is_empty())
    {
        extras.push(format!("full_path={full_path}"));
    }
    if let Some(app) = voice.app.as_deref() {
        extras.push(format!("app={app}"));
    }
    if let Some(md5) = voice.md5.as_deref() {
        extras.push(format!("md5={md5}"));
    }
    if let Some(file_id) = voice.file_id {
        extras.push(format!("file_id={file_id}"));
    }
    if let Some(file_key) = voice.file_key.as_deref() {
        extras.push(format!("file_key={file_key}"));
    }
    if let Some(hash) = voice.hash.as_deref() {
        extras.push(format!("hash={hash}"));
    }
    if let Some(transcribed_text) = voice
        .transcribed_text
        .as_deref()
        .filter(|text| !text.is_empty() && *text != voice.preview)
    {
        extras.push(format!("text={transcribed_text}"));
    }

    if extras.is_empty() {
        voice.preview.clone()
    } else {
        format!("{} {{{}}}", voice.preview, extras.join(","))
    }
}

fn render_voice_part_compact(voice: &VoicePart) -> String {
    let path = voice
        .path
        .as_deref()
        .filter(|path| !path.is_empty())
        .or_else(|| {
            voice
                .full_path
                .as_deref()
                .filter(|full_path| !full_path.is_empty())
        });
    if let Some(path) = path {
        return format!("[语音] path={path}");
    }
    voice.preview.clone()
}

fn parse_voice_part_compact(payload: &[u8], info: Option<&InfoMap>) -> Option<String> {
    collect_voice_part_compact(payload, info)
}

fn parse_video_part(payload: &[u8], info: Option<&InfoMap>) -> Option<VideoPart> {
    if let Some(nested) = parse_nested_td_payload(payload) {
        if let Some(video) = collect_video_part(&nested) {
            return Some(video);
        }
    }

    info.and_then(|info| {
        info.text("MSGBOX_bsPreviewMsgText")
            .map(|preview| VideoPart {
                title: preview.to_string(),
                ..VideoPart::default()
            })
    })
}

fn render_video_part(video: &VideoPart) -> String {
    let title = if !video.title.is_empty() {
        video.title.clone()
    } else {
        "[视频]".to_string()
    };

    let mut extras = Vec::new();
    if let Some(size_bytes) = video.size_bytes {
        extras.push(format!("size={size_bytes}Byte"));
    }
    if let Some(path) = video.path.as_deref() {
        extras.push(format!("path={path}"));
    }
    if let Some(thumb_path) = video.thumb_path.as_deref() {
        extras.push(format!("thumb={thumb_path}"));
    }
    if let Some((width, height)) = video.thumb_size {
        extras.push(format!("thumb_size={width}x{height}"));
    }
    if let Some(thumb_file_size) = video.thumb_file_size {
        extras.push(format!("thumb_file_size={thumb_file_size}Byte"));
    }
    if let Some(md5) = video.md5.as_deref() {
        extras.push(format!("md5={md5}"));
    }
    if let Some(source) = video.source.as_deref() {
        extras.push(format!("source={source}"));
    }

    if extras.is_empty() {
        title
    } else {
        format!("{title} {{{}}}", extras.join(","))
    }
}

fn render_video_part_compact(video: &VideoPart) -> String {
    let title = if !video.title.is_empty() && !video.title.contains("视频消息") {
        video.title.clone()
    } else {
        "[视频]".to_string()
    };

    let mut extras = Vec::new();
    if let Some(path) = video.path.as_deref().filter(|path| !path.is_empty()) {
        extras.push(format!("path={path}"));
    }
    if let Some(thumb_path) = video
        .thumb_path
        .as_deref()
        .filter(|thumb_path| !thumb_path.is_empty())
    {
        extras.push(format!("thumb={thumb_path}"));
    }

    if extras.is_empty() {
        title
    } else {
        format!("{title} {}", extras.join(" "))
    }
}

fn parse_video_part_compact(payload: &[u8], info: Option<&InfoMap>) -> Option<String> {
    if let Some(nested) = parse_nested_td_payload(payload) {
        if let Some(video) = collect_video_part_compact(&nested) {
            return Some(video);
        }
    }

    info.and_then(|info| {
        collect_video_part_compact(info).or_else(|| {
            info.text("MSGBOX_bsPreviewMsgText")
                .map(|preview| preview.to_string())
        })
    })
}

fn parse_transfer_part(payload: &[u8], info: Option<&InfoMap>) -> Option<TransferPart> {
    let utf16_strings = extract_utf16le_strings(payload);
    let file_name = utf16_strings
        .iter()
        .find(|text| looks_like_filename(text))
        .cloned();
    if let Some(file_name) = file_name {
        return Some(TransferPart {
            text: format!("成功接收离线文件“{file_name}”"),
            file_name: Some(file_name),
            app_hint: None,
        });
    }
    if let Some(info) = info {
        if let Some(path) = info.text("strFileSelect") {
            let file_name = file_name_from_path(path);
            if !file_name.is_empty() {
                return Some(TransferPart {
                    text: format!("成功接收离线文件“{file_name}”"),
                    file_name: Some(file_name),
                    app_hint: None,
                });
            }
        }
    }
    if utf16_strings
        .iter()
        .any(|text| text.contains("com.tencent.filetransfer"))
    {
        return Some(TransferPart {
            text: "[离线文件,app=com.tencent.filetransfer]".to_string(),
            file_name: None,
            app_hint: Some("com.tencent.filetransfer".to_string()),
        });
    }
    Some(TransferPart {
        text: "[离线文件]".to_string(),
        file_name: None,
        app_hint: None,
    })
}

fn parse_share_part(payload: &[u8]) -> Option<SharePart> {
    collect_share_part(payload)
}

fn render_share_part(share: &SharePart) -> String {
    let brief = share
        .brief
        .as_deref()
        .filter(|brief| !brief.is_empty() && *brief != "[分享]")
        .unwrap_or("[分享]");
    let mut out = brief.to_string();
    if let Some(title) = share.title.as_deref().filter(|title| !title.is_empty()) {
        out.push(' ');
        out.push_str(title);
    }
    if let Some(summary) = share
        .summary
        .as_deref()
        .filter(|summary| !summary.is_empty() && share.title.as_deref() != Some(*summary))
    {
        out.push_str(" - ");
        out.push_str(summary);
    }
    if let Some(source_name) = share
        .source_name
        .as_deref()
        .filter(|source_name| !source_name.is_empty())
    {
        out.push_str(" (");
        out.push_str(source_name);
        out.push(')');
    }
    if let Some(app_hint) = share
        .app_hint
        .as_deref()
        .filter(|app_hint| !app_hint.is_empty())
    {
        out.push_str(" {app=");
        out.push_str(app_hint);
        out.push('}');
    }
    if let Some(url) = share.url.as_deref().filter(|url| !url.is_empty()) {
        out.push(' ');
        out.push_str(url);
    }
    if out.trim().is_empty() {
        "[分享]".to_string()
    } else {
        out
    }
}

fn parse_file_part(payload: &[u8], info: Option<&InfoMap>) -> Option<FileInfo> {
    collect_file_part(payload, info)
}

fn parse_td_27_part(payload: &[u8]) -> Option<SharePart> {
    if let Some(info) = parse_nested_td_payload(payload) {
        let prompt = info.text("prompt").filter(|text| !text.trim().is_empty());
        let brief = info.text("brief").filter(|text| !text.trim().is_empty());
        let title = info.text("title").filter(|text| !text.trim().is_empty());
        if prompt.is_some() || brief.is_some() || title.is_some() {
            return Some(SharePart {
                brief: prompt.or(brief).map(str::to_string),
                title: title.map(str::to_string),
                ..SharePart::default()
            });
        }
    }

    Some(SharePart {
        brief: Some("[分享]".to_string()),
        summary: {
            let hints = binary_payload_hints(payload);
            (!hints.is_empty()).then(|| hints.join(" | "))
        },
        ..SharePart::default()
    })
}

fn parse_td_30_part(payload: &[u8], _info: Option<&InfoMap>) -> Option<ForwardRecordPart> {
    collect_forward_record(payload)
}

fn decode_text_payload(payload: &[u8]) -> String {
    let mut out = String::new();
    append_decoded_text_payload(&mut out, payload);
    out
}

fn append_decoded_text_payload(out: &mut String, payload: &[u8]) {
    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x01 => append_utf16le_to_string(out, data),
            0x06 => {
                if let Ok(at) = TextElement::at_from_raw_db(data) {
                    out.push_str(&at.to_string());
                }
            }
            0x00 | 0x02 | 0x03 | 0x08 => {}
            _ => {}
        }
        ptr += len;
    }
}

fn decode_face_payload(payload: &[u8]) -> String {
    let mut ptr = 0;
    let mut face_id = None;
    let mut fallback_name = None;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if inner_type == 0x01 {
            let mut id = 0_u64;
            for byte in data {
                id = (id << 8) | (*byte as u64);
            }
            face_id = Some(id);
        } else if inner_type == 0x02 {
            fallback_name = extract_face_name_from_extra(data);
        }
        ptr += len;
    }
    if let Some(name) = fallback_name.as_deref() {
        return format!("[{name}]");
    }
    if let Some(id) = face_id {
        if let Some(name) = face_name(id) {
            return format!("[{name}]");
        }
        return format!("[表情:{id}]");
    }
    "[表情]".to_string()
}

fn extract_face_name_from_extra(data: &[u8]) -> Option<String> {
    let mut ptr = 0;
    while let Ok((_, value)) = next_proto_field(data, &mut ptr) {
        if let ProtoValue::Len(bytes) = value {
            if let Ok(text) = std::str::from_utf8(bytes) {
                if let Some(name) = text.strip_prefix('/') {
                    if !name.is_empty() {
                        return Some(name.to_string());
                    }
                }
            }
        }
        if ptr >= data.len() {
            break;
        }
    }
    None
}

fn face_name(id: u64) -> Option<&'static str> {
    match id {
        0 => Some("惊讶"),
        1 => Some("撇嘴"),
        2 => Some("色"),
        3 => Some("发呆"),
        4 => Some("得意"),
        5 => Some("流泪"),
        6 => Some("害羞"),
        7 => Some("闭嘴"),
        8 => Some("睡"),
        9 => Some("大哭"),
        10 => Some("尴尬"),
        11 => Some("发怒"),
        12 => Some("调皮"),
        13 => Some("呲牙"),
        14 => Some("微笑"),
        15 => Some("难过"),
        16 => Some("酷"),
        18 => Some("抓狂"),
        19 => Some("吐"),
        20 => Some("偷笑"),
        21 => Some("可爱"),
        22 => Some("白眼"),
        23 => Some("傲慢"),
        24 => Some("饥饿"),
        25 => Some("困"),
        26 => Some("惊恐"),
        27 => Some("流汗"),
        28 => Some("憨笑"),
        29 => Some("悠闲"),
        30 => Some("奋斗"),
        31 => Some("咒骂"),
        32 => Some("疑问"),
        33 => Some("嘘"),
        34 => Some("晕"),
        35 => Some("折磨"),
        36 => Some("衰"),
        37 => Some("骷髅"),
        38 => Some("敲打"),
        39 => Some("再见"),
        41 => Some("发抖"),
        42 => Some("爱情"),
        43 => Some("跳跳"),
        46 => Some("猪头"),
        49 => Some("拥抱"),
        53 => Some("蛋糕"),
        54 => Some("闪电"),
        55 => Some("炸弹"),
        56 => Some("刀"),
        57 => Some("足球"),
        59 => Some("便便"),
        60 => Some("咖啡"),
        61 => Some("饭"),
        63 => Some("玫瑰"),
        64 => Some("凋谢"),
        66 => Some("爱心"),
        67 => Some("心碎"),
        69 => Some("礼物"),
        74 => Some("太阳"),
        75 => Some("月亮"),
        76 => Some("赞"),
        77 => Some("踩"),
        78 => Some("握手"),
        79 => Some("胜利"),
        85 => Some("飞吻"),
        86 => Some("怄火"),
        89 => Some("西瓜"),
        96 => Some("冷汗"),
        97 => Some("擦汗"),
        98 => Some("抠鼻"),
        99 => Some("鼓掌"),
        100 => Some("糗大了"),
        101 => Some("坏笑"),
        102 => Some("左哼哼"),
        103 => Some("右哼哼"),
        104 => Some("哈欠"),
        105 => Some("鄙视"),
        106 => Some("委屈"),
        107 => Some("快哭了"),
        108 => Some("阴险"),
        109 => Some("左亲亲"),
        110 => Some("吓"),
        111 => Some("可怜"),
        112 => Some("菜刀"),
        113 => Some("啤酒"),
        114 => Some("篮球"),
        115 => Some("乒乓"),
        116 => Some("示爱"),
        117 => Some("瓢虫"),
        118 => Some("抱拳"),
        119 => Some("勾引"),
        120 => Some("拳头"),
        121 => Some("差劲"),
        122 => Some("爱你"),
        123 => Some("NO"),
        124 => Some("OK"),
        125 => Some("转圈"),
        126 => Some("磕头"),
        127 => Some("回头"),
        128 => Some("跳绳"),
        129 => Some("挥手"),
        130 => Some("激动"),
        131 => Some("街舞"),
        132 => Some("献吻"),
        133 => Some("左太极"),
        134 => Some("右太极"),
        136 => Some("双喜"),
        137 => Some("鞭炮"),
        138 => Some("灯笼"),
        140 => Some("K歌"),
        144 => Some("喝彩"),
        145 => Some("祈祷"),
        146 => Some("爆筋"),
        147 => Some("棒棒糖"),
        148 => Some("喝奶"),
        151 => Some("飞机"),
        158 => Some("钞票"),
        168 => Some("药"),
        169 => Some("手枪"),
        171 => Some("茶"),
        172 => Some("眨眼睛"),
        173 => Some("泪奔"),
        174 => Some("无奈"),
        175 => Some("卖萌"),
        176 => Some("小纠结"),
        177 => Some("喷血"),
        178 => Some("斜眼笑"),
        179 => Some("doge"),
        180 => Some("惊喜"),
        181 => Some("骚扰"),
        182 => Some("笑哭"),
        183 => Some("我最美"),
        184 => Some("河蟹"),
        185 => Some("羊驼"),
        187 => Some("幽灵"),
        188 => Some("蛋"),
        190 => Some("菊花"),
        192 => Some("红包"),
        193 => Some("大笑"),
        194 => Some("不开心"),
        197 => Some("冷漠"),
        198 => Some("呃"),
        199 => Some("好棒"),
        200 => Some("拜托"),
        201 => Some("点赞"),
        202 => Some("无聊"),
        203 => Some("托脸"),
        204 => Some("吃"),
        205 => Some("送花"),
        206 => Some("害怕"),
        207 => Some("花痴"),
        208 => Some("小样儿"),
        210 => Some("飙泪"),
        211 => Some("我不看"),
        212 => Some("托腮"),
        214 => Some("啵啵"),
        215 => Some("糊脸"),
        216 => Some("拍头"),
        217 => Some("扯一扯"),
        218 => Some("舔一舔"),
        219 => Some("蹭一蹭"),
        220 => Some("拽炸天"),
        221 => Some("顶呱呱"),
        222 => Some("抱抱"),
        223 => Some("暴击"),
        224 => Some("开枪"),
        225 => Some("撩一撩"),
        226 => Some("拍桌"),
        227 => Some("拍手"),
        228 => Some("恭喜"),
        229 => Some("干杯"),
        230 => Some("嘲讽"),
        231 => Some("哼"),
        232 => Some("佛系"),
        233 => Some("掐一掐"),
        234 => Some("惊呆"),
        235 => Some("颤抖"),
        236 => Some("啃头"),
        237 => Some("偷看"),
        238 => Some("扇脸"),
        239 => Some("原谅"),
        240 => Some("喷脸"),
        241 => Some("生日快乐"),
        242 => Some("头撞击"),
        243 => Some("甩头"),
        244 => Some("扔狗"),
        245 => Some("加油必胜"),
        246 => Some("加油抱抱"),
        247 => Some("口罩护体"),
        260 => Some("搬砖中"),
        261 => Some("忙到飞起"),
        262 => Some("脑阔疼"),
        263 => Some("沧桑"),
        264 => Some("捂脸"),
        265 => Some("辣眼睛"),
        266 => Some("哦哟"),
        267 => Some("头秃"),
        268 => Some("问号脸"),
        269 => Some("暗中观察"),
        270 => Some("emm"),
        271 => Some("吃瓜"),
        272 => Some("呵呵哒"),
        273 => Some("我酸了"),
        274 => Some("太南了"),
        276 => Some("辣椒酱"),
        277 => Some("汪汪"),
        278 => Some("汗"),
        279 => Some("打脸"),
        280 => Some("击掌"),
        281 => Some("无眼笑"),
        282 => Some("敬礼"),
        283 => Some("狂笑"),
        284 => Some("面无表情"),
        285 => Some("摸鱼"),
        286 => Some("魔鬼笑"),
        287 => Some("哦"),
        288 => Some("请"),
        289 => Some("睁眼"),
        290 => Some("敲开心"),
        291 => Some("震惊"),
        292 => Some("让我康康"),
        293 => Some("摸锦鲤"),
        294 => Some("期待"),
        295 => Some("拿到红包"),
        296 => Some("真好"),
        297 => Some("拜谢"),
        298 => Some("元宝"),
        299 => Some("牛啊"),
        300 => Some("胖三斤"),
        301 => Some("好闪"),
        302 => Some("左拜年"),
        303 => Some("右拜年"),
        304 => Some("红包包"),
        305 => Some("右亲亲"),
        306 => Some("牛气冲天"),
        307 => Some("喵喵"),
        308 => Some("求红包"),
        309 => Some("谢红包"),
        310 => Some("新年烟花"),
        311 => Some("打call"),
        312 => Some("变形"),
        313 => Some("嗑到了"),
        314 => Some("仔细分析"),
        315 => Some("加油"),
        316 => Some("我没事"),
        317 => Some("菜汪"),
        318 => Some("崇拜"),
        319 => Some("比心"),
        320 => Some("庆祝"),
        321 => Some("老色痞"),
        322 => Some("拒绝"),
        323 => Some("嫌弃"),
        324 => Some("吃糖"),
        325 => Some("惊吓"),
        326 => Some("生气"),
        327 => Some("加一"),
        328 => Some("错号"),
        329 => Some("对号"),
        330 => Some("完成"),
        331 => Some("明白"),
        _ => None,
    }
}

#[allow(dead_code)]
fn decode_image_payload(payload: &[u8], label: &str, ext_info: Option<&ExtImageInfo>) -> String {
    parse_image_part(payload, label, ext_info)
        .map(|part| render_image_part(&part))
        .unwrap_or_else(|| format!("[{label}]"))
}

fn image_summary_from_info(info: &InfoMap) -> Option<String> {
    [
        "strTip",
        "str_text_summary",
        "bytes_text_summary",
        "strSummary",
        "bytes_summary",
    ]
    .into_iter()
    .find_map(|key| {
        info.raw(key)
            .and_then(decode_image_summary_raw)
            .or_else(|| {
                info.text(key)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(|text| text.to_string())
            })
    })
}

fn normalize_bracketed_name(text: &str) -> &str {
    let trimmed = text.trim();
    trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(trimmed)
}

fn decode_image_summary_raw(raw: &[u8]) -> Option<String> {
    if raw.is_empty() {
        return None;
    }

    if raw.len() % 2 == 0 {
        let utf16 = decode_utf16le(raw).trim().trim_matches('\0').to_string();
        if !utf16.is_empty() {
            return Some(utf16);
        }
    }

    std::str::from_utf8(raw)
        .ok()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(|text| text.to_string())
}

fn decode_textish_raw(raw: &[u8]) -> Option<String> {
    if raw.is_empty() {
        return None;
    }

    let utf8 = std::str::from_utf8(raw)
        .ok()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(|text| text.replace('\0', ""));

    let utf16 = if raw.len() % 2 == 0 {
        let text = decode_utf16le(raw).trim().trim_matches('\0').to_string();
        (!text.is_empty()).then_some(text)
    } else {
        None
    };

    match (utf8, utf16) {
        (Some(utf8), Some(utf16)) => {
            if is_likely_utf16_bytes(raw) && is_plausible_textish(&utf16) {
                Some(utf16)
            } else if is_plausible_textish(&utf8) {
                Some(utf8)
            } else if is_plausible_textish(&utf16) {
                Some(utf16)
            } else {
                Some(utf8)
            }
        }
        (Some(utf8), None) => Some(utf8),
        (None, Some(utf16)) => Some(utf16),
        (None, None) => None,
    }
}

fn is_likely_utf16_bytes(raw: &[u8]) -> bool {
    if raw.len() < 4 || raw.len() % 2 != 0 {
        return false;
    }
    let odd_zeros = raw
        .iter()
        .skip(1)
        .step_by(2)
        .filter(|byte| **byte == 0)
        .count();
    let even_zeros = raw.iter().step_by(2).filter(|byte| **byte == 0).count();
    odd_zeros * 3 >= raw.len() / 2 || even_zeros * 3 >= raw.len() / 2
}

fn is_plausible_textish(text: &str) -> bool {
    let total = text.chars().count();
    if total == 0 {
        return false;
    }
    let plausible = text
        .chars()
        .filter(|ch| {
            ch.is_ascii_graphic()
                || *ch == ' '
                || is_cjk(*ch)
                || matches!(
                    ch,
                    '[' | ']'
                        | '('
                        | ')'
                        | '{'
                        | '}'
                        | '-'
                        | '_'
                        | '.'
                        | '/'
                        | '\\'
                        | ':'
                        | '：'
                        | '，'
                        | '。'
                        | '！'
                        | '？'
                        | '“'
                        | '”'
                        | '‘'
                        | '’'
                        | '《'
                        | '》'
                        | '@'
                        | '#'
                        | '&'
                        | '+'
                        | '='
                )
        })
        .count();
    plausible * 10 >= total * 7
}

fn looks_like_md5(text: &str) -> bool {
    text.len() == 32 && text.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn looks_like_json(text: &str) -> bool {
    let trimmed = text.trim();
    (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
}

fn normalize_json_text(text: &str) -> Option<String> {
    serde_json::from_str::<JsonValue>(text)
        .ok()
        .and_then(|value| serde_json::to_string(&value).ok())
}

fn decode_ext_image_payload(payload: &[u8]) -> Option<ExtImageInfo> {
    let mut info = ExtImageInfo::default();
    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x01 => {
                let name = decode_utf16le(data)
                    .trim()
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .trim()
                    .to_string();
                if !name.is_empty() {
                    info.name = Some(name);
                }
            }
            0x04 => {
                if let Some((width, height)) = parse_ext_image_dimensions(data) {
                    info.width = Some(width);
                    info.height = Some(height);
                }
            }
            _ => {}
        }
        ptr += len;
    }

    if info.name.is_some() || info.width.is_some() || info.height.is_some() {
        Some(info)
    } else {
        None
    }
}

fn parse_ext_image_dimensions(data: &[u8]) -> Option<(u16, u16)> {
    if data.len() != 4 {
        return None;
    }

    let width_be = u16::from_be_bytes([data[0], data[1]]);
    let height_be = u16::from_be_bytes([data[2], data[3]]);
    if (1..=4096).contains(&width_be) && (1..=4096).contains(&height_be) {
        return Some((width_be, height_be));
    }

    let width_le = u16::from_le_bytes([data[0], data[1]]);
    let height_le = u16::from_le_bytes([data[2], data[3]]);
    if (1..=4096).contains(&width_le) && (1..=4096).contains(&height_le) {
        return Some((width_le, height_le));
    }

    None
}

#[allow(dead_code)]
fn decode_voice_payload(payload: &[u8], info: Option<&InfoMap>) -> String {
    parse_voice_part(payload, info)
        .map(|part| render_voice_part(&part))
        .unwrap_or_else(|| "[语音]".to_string())
}

#[allow(dead_code)]
fn decode_voice_tlv_payload(payload: &[u8], info: Option<&InfoMap>) -> Option<String> {
    let mut ptr = 0;
    let mut preview = info
        .and_then(|value| value.text("MSGBOX_bsPreviewMsgText"))
        .map(str::to_string);
    let mut path = None;
    let mut app = None;
    let mut hash = None;
    let mut file_key = None;

    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x00 | 0x03 => {
                if let Some(nested) = parse_nested_td_payload(data) {
                    if preview.is_none() {
                        preview = nested
                            .text("MSGBOX_bsPreviewMsgText")
                            .or_else(|| nested.text("bsAbstractText"))
                            .map(str::to_string);
                    }
                    if path.is_none() {
                        path = nested
                            .raw("FilePath")
                            .and_then(decode_image_summary_raw)
                            .or_else(|| nested.raw("bsFullPath").and_then(decode_image_summary_raw))
                            .or_else(|| {
                                nested
                                    .text("FilePath")
                                    .or_else(|| nested.text("bsFullPath"))
                                    .map(str::to_string)
                            });
                    }
                    if file_key.is_none() {
                        file_key = nested.text("bufFileKey").map(str::to_string);
                    }
                }
            }
            0x01 => hash = Some(hex_string(data)),
            0x02 => {
                let package = decode_utf16le(data).trim().trim_matches('\0').to_string();
                if !package.is_empty() {
                    app = Some(package);
                }
            }
            _ => {}
        }
        ptr += len;
    }

    let mut extras = Vec::new();
    if let Some(path) = path {
        extras.push(format!("path={path}"));
    }
    if let Some(app) = app {
        extras.push(format!("app={app}"));
    }
    if let Some(md5) = info.and_then(|value| value.text("bsGroupAudioMd5")) {
        extras.push(format!("md5={md5}"));
    }
    if let Some(file_id) = info.and_then(|value| value.u32("dwGroupFileID")) {
        extras.push(format!("file_id={file_id}"));
    }
    if let Some(file_key) = file_key {
        extras.push(format!("file_key={file_key}"));
    }
    if let Some(hash) = hash {
        extras.push(format!("hash={hash}"));
    }

    preview.map(|preview| {
        if extras.is_empty() {
            preview
        } else {
            format!("{preview} {{{}}}", extras.join(","))
        }
    })
}

#[allow(dead_code)]
fn decode_video_payload(payload: &[u8], info: Option<&InfoMap>) -> String {
    parse_video_part(payload, info)
        .map(|part| render_video_part(&part))
        .unwrap_or_else(|| "[视频]".to_string())
}

#[allow(dead_code)]
fn decode_video_td_payload(info: &InfoMap) -> Option<String> {
    let path = info.text("strVideoFilePath").map(|value| value.to_string());
    let thumb_path = info
        .text("strVideoThumbPath")
        .map(|value| value.to_string());
    let file_name = info
        .text("bytes_file_name")
        .and_then(|name| {
            let candidate = if name.contains(['\\', '/']) {
                file_name_from_path(name)
            } else {
                name.to_string()
            };
            looks_like_filename(&candidate).then_some(candidate)
        })
        .or_else(|| {
            path.as_deref()
                .map(file_name_from_path)
                .filter(|name| !name.is_empty())
        });
    let file_size = info.u32("uint32_file_size");
    let md5 = info.raw("bytes_file_md5").map(hex_string);
    let thumb_width = info.u32("uint32_thumb_width");
    let thumb_height = info.u32("uint32_thumb_height");
    let source = info.text("bytes_source").map(str::to_string);

    if file_name.is_none()
        && path.is_none()
        && thumb_path.is_none()
        && md5.is_none()
        && thumb_width.is_none()
        && thumb_height.is_none()
        && source.is_none()
    {
        return None;
    }

    let title = file_name
        .map(|name| format!("视频\"{name}\""))
        .unwrap_or_else(|| "[视频]".to_string());

    let mut extras = Vec::new();
    if let Some(size) = file_size {
        extras.push(format!("size={size}Byte"));
    }
    if let Some(path) = path {
        extras.push(format!("path={path}"));
    }
    if let Some(thumb_path) = thumb_path {
        extras.push(format!("thumb={thumb_path}"));
    }
    if let (Some(width), Some(height)) = (thumb_width, thumb_height) {
        extras.push(format!("thumb_size={width}x{height}"));
    }
    if let Some(md5) = md5 {
        extras.push(format!("md5={md5}"));
    }
    if let Some(source) = source {
        extras.push(format!("source={source}"));
    }

    if extras.is_empty() {
        Some(title)
    } else {
        Some(format!("{title} {{{}}}", extras.join(",")))
    }
}

fn decode_emoji_payload(payload: &[u8]) -> String {
    let mut ptr = 0;
    let mut numeric_face_id = None;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if inner_type == 0x01 {
            let text = decode_utf16le(data);
            if !text.trim().is_empty() {
                return text;
            }
        } else if numeric_face_id.is_none() {
            numeric_face_id = decode_face_id_candidate(data);
        }
        ptr += len;
    }
    if let Some(face_id) = numeric_face_id.and_then(face_name) {
        return format!("[{face_id}]");
    }
    String::new()
}

fn decode_face_id_candidate(data: &[u8]) -> Option<u64> {
    let value = match data.len() {
        1 => Some(data[0] as u64),
        2 => Some(u16::from_le_bytes([data[0], data[1]]) as u64),
        4 => Some(u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as u64),
        8 => Some(u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ])),
        _ => None,
    }?;
    face_name(value).map(|_| value)
}

#[allow(dead_code)]
fn decode_transfer_payload(payload: &[u8], info: Option<&InfoMap>) -> String {
    parse_transfer_part(payload, info)
        .map(|part| part.text)
        .unwrap_or_else(|| "[离线文件]".to_string())
}

#[allow(dead_code)]
fn decode_share_payload(payload: &[u8]) -> String {
    parse_share_part(payload)
        .map(|part| render_share_part(&part))
        .unwrap_or_else(|| "[分享]".to_string())
}

#[allow(dead_code)]
fn decode_file_payload(payload: &[u8], info: Option<&InfoMap>) -> String {
    parse_file_part(payload, info)
        .map(|part| format_file_info(&part))
        .unwrap_or_else(|| "[文件]".to_string())
}

fn parse_file_proto(data: &[u8]) -> FileInfo {
    let mut info = FileInfo::default();
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        match (field, value) {
            (2, ProtoValue::Len(bytes)) => {
                info.filename = String::from_utf8_lossy(bytes).into_owned()
            }
            (3, ProtoValue::Len(bytes)) => {
                info.size_text = String::from_utf8_lossy(bytes).into_owned()
            }
            (7, ProtoValue::Len(bytes)) => parse_file_proto_level2(bytes, &mut info),
            _ => {}
        }
        if ptr >= data.len() {
            break;
        }
    }
    info
}

fn parse_file_proto_level2(data: &[u8], info: &mut FileInfo) {
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        if let (2, ProtoValue::Len(bytes)) = (field, value) {
            parse_file_proto_level3(bytes, info);
        }
        if ptr >= data.len() {
            break;
        }
    }
}

fn parse_file_proto_level3(data: &[u8], info: &mut FileInfo) {
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        match (field, value) {
            (2, ProtoValue::Len(bytes)) => info.path = String::from_utf8_lossy(bytes).into_owned(),
            (3, ProtoValue::Varint(size)) => info.size_bytes = Some(size),
            (4, ProtoValue::Len(bytes)) => {
                if info.filename.is_empty() {
                    info.filename = String::from_utf8_lossy(bytes).into_owned();
                }
            }
            (7, ProtoValue::Len(bytes)) => {
                let text = String::from_utf8_lossy(bytes).into_owned();
                info.extra_json = normalize_json_text(&text).unwrap_or(text);
            }
            (8, ProtoValue::Len(bytes)) => info.md5 = String::from_utf8_lossy(bytes).into_owned(),
            _ => {}
        }
        if ptr >= data.len() {
            break;
        }
    }
}

fn format_file_info(info: &FileInfo) -> String {
    let title = if !info.filename.is_empty() {
        format!("分享文件\"{}\"", info.filename)
    } else {
        "[分享文件]".to_string()
    };

    let mut extras = Vec::new();
    if !info.size_text.is_empty() {
        extras.push(format!("size={}", info.size_text));
    } else if let Some(size) = info.size_bytes {
        extras.push(format!("size={size}Byte"));
    }
    if !info.path.is_empty() {
        extras.push(format!("path={}", info.path));
    }
    if !info.extra_json.is_empty() {
        extras.push(format!("meta={}", info.extra_json));
    }
    if !info.md5.is_empty() {
        extras.push(format!("md5={}", info.md5));
    }
    if extras.is_empty() {
        title
    } else {
        format!("{title} {{{}}}", extras.join(","))
    }
}

fn format_file_info_compact(info: &FileInfo) -> String {
    if !info.path.is_empty() {
        return format!("[文件] path={}", info.path);
    }
    if !info.filename.is_empty() {
        return format!("[文件] name={}", info.filename);
    }
    "[文件]".to_string()
}

fn parse_file_part_compact(payload: &[u8], info: Option<&InfoMap>) -> Option<String> {
    collect_file_part_compact(payload, info)
}

fn patch_file_info_from_info(info: &mut FileInfo, extra: Option<&InfoMap>) {
    let Some(extra) = extra else {
        return;
    };

    if let Some(path) = extra.textish("strFileSelect") {
        if info.path.is_empty() {
            info.path = path.to_string();
        }
        if info.filename.is_empty() {
            info.filename = file_name_from_path(&path);
        }
    }
}

fn inflate_to_string(data: &[u8]) -> Result<String> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = String::new();
    decoder.read_to_string(&mut out)?;
    Ok(out)
}

fn decode_nickname(payload: &[u8]) -> Option<String> {
    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if matches!(inner_type, 0x01 | 0x02) {
            return Some(decode_utf16le(data));
        }
        ptr += len;
    }
    None
}

#[allow(dead_code)]
fn decode_td_27_payload(payload: &[u8]) -> String {
    parse_td_27_part(payload)
        .map(|part| render_share_part(&part))
        .unwrap_or_else(|| decode_binary_hint_payload(payload, "[分享]"))
}

#[allow(dead_code)]
fn decode_td_30_payload(payload: &[u8], _info: Option<&InfoMap>) -> String {
    parse_td_30_part(payload, _info)
        .map(|part| render_forward_record(&part))
        .unwrap_or_else(|| {
            let hints = binary_payload_hints(payload);
            if hints.is_empty() {
                "[聊天记录]".to_string()
            } else {
                format!("[聊天记录] {}", hints.join(" | "))
            }
        })
}

fn format_forward_brief(brief: Option<&str>, title: Option<&str>) -> String {
    let brief = brief.map(str::trim).filter(|text| !text.is_empty());
    let title = title.map(str::trim).filter(|text| !text.is_empty());

    match (brief, title) {
        (Some("[聊天记录]"), Some(title)) if title.contains("聊天记录") => {
            format!("[{title}]")
        }
        (Some(brief), Some(title)) if brief == title => brief.to_string(),
        (Some(brief), Some(title)) => format!("{brief} {title}"),
        (Some(brief), None) => brief.to_string(),
        (None, Some(title)) if title.contains("聊天记录") => format!("[{title}]"),
        (None, Some(title)) => format!("[聊天记录] {title}"),
        _ => "[聊天记录]".to_string(),
    }
}

fn collect_image_part(
    payload: &[u8],
    label: &str,
    ext_info: Option<&ExtImageInfo>,
) -> Option<ImagePart> {
    let mut ptr = 0;
    let mut path = None;
    let mut hash = None;
    let mut summary = None;
    let mut customface_type = None;
    let mut biz_type = None;

    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x01 => hash = Some(hex_string(data)),
            0x02 => path = decode_textish_raw(data),
            0x00 | 0x0a => {
                if let Some(nested) = parse_nested_td_payload(data) {
                    if summary.is_none() {
                        summary = image_summary_from_info(&nested);
                    }
                    if path.is_none() {
                        path = nested
                            .textish("strFileName")
                            .or_else(|| nested.textish("FilePath"))
                            .or_else(|| nested.textish("bsFullPath"));
                    }
                    customface_type = customface_type
                        .or_else(|| nested.u32("uint32_customface_type"))
                        .or_else(|| nested.u32("wAppId"));
                    biz_type = biz_type.or_else(|| nested.u32("uint32_image_biz_type"));
                }
            }
            0x08 | 0x0b => {
                if len == 4 {
                    let value = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                    customface_type.get_or_insert(value);
                }
            }
            _ => {}
        }
        ptr += len;
    }

    let display_label = if let Some(summary_text) = summary.as_deref() {
        normalize_bracketed_name(summary_text).to_string()
    } else if let Some(ext_info) = ext_info {
        ext_info
            .name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .map(normalize_bracketed_name)
            .map(str::to_string)
            .unwrap_or_else(|| default_image_label(path.as_deref(), label))
    } else {
        default_image_label(path.as_deref(), label)
    };

    let name = ext_info
        .and_then(|ext_info| ext_info.name.clone())
        .or_else(|| {
            summary.clone().and_then(|text| {
                let normalized = normalize_bracketed_name(&text);
                if normalized == display_label {
                    None
                } else {
                    Some(normalized.to_string())
                }
            })
        });

    Some(ImagePart {
        label: display_label,
        name,
        summary,
        path,
        hash,
        size_text: ext_info.and_then(ExtImageInfo::size_text),
        customface_type,
        biz_type,
    })
}

fn collect_image_part_compact(
    payload: &[u8],
    label: &str,
    ext_info: Option<&ExtImageInfo>,
) -> Option<String> {
    let mut ptr = 0;
    let mut path = None;
    let mut summary = None;

    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x02 => path = decode_textish_raw(data),
            0x00 | 0x0a => {
                if let Some(nested) = parse_nested_td_payload(data) {
                    if summary.is_none() {
                        summary = image_summary_from_info(&nested);
                    }
                    if path.is_none() {
                        path = nested
                            .textish("strFileName")
                            .or_else(|| nested.textish("FilePath"))
                            .or_else(|| nested.textish("bsFullPath"));
                    }
                }
            }
            _ => {}
        }
        ptr += len;
    }

    let display_label = if let Some(summary_text) = summary.as_deref() {
        normalize_bracketed_name(summary_text).to_string()
    } else if let Some(ext_info) = ext_info {
        ext_info
            .name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .map(normalize_bracketed_name)
            .map(str::to_string)
            .unwrap_or_else(|| default_image_label(path.as_deref(), label))
    } else {
        default_image_label(path.as_deref(), label)
    };

    let out = if let Some(path) = path.as_deref().filter(|path| !path.is_empty()) {
        format!("[{display_label}] path={path}")
    } else {
        format!("[{display_label}]")
    };
    let animated_emoji = should_render_animated_emoji_compact(
        summary.as_deref(),
        ext_info.and_then(|info| info.name.as_deref()),
        path.as_deref(),
    );
    if animated_emoji {
        Some(render_animated_emoji_compact(
            display_label.as_str(),
            path.as_deref(),
        ))
    } else {
        Some(out)
    }
}

fn should_render_animated_emoji_compact(
    summary_hint: Option<&str>,
    ext_name_hint: Option<&str>,
    path: Option<&str>,
) -> bool {
    is_animated_emoji_path(path)
        || has_explicit_animated_emoji_hint(summary_hint)
        || has_explicit_animated_emoji_hint(ext_name_hint)
}

fn has_explicit_animated_emoji_hint(text: Option<&str>) -> bool {
    text.map(is_explicit_animated_emoji_label).unwrap_or(false)
}

fn is_explicit_animated_emoji_label(text: &str) -> bool {
    let normalized = normalize_bracketed_name(text);
    !normalized.is_empty() && normalized.contains("动画表情")
}

fn is_animated_emoji_path(path: Option<&str>) -> bool {
    matches!(path, Some(path) if path.starts_with("UserDataCustomFace:") || path.starts_with("FaceStore:"))
}

fn render_animated_emoji_compact(display_label: &str, path: Option<&str>) -> String {
    let mut extras = Vec::new();
    let display_label = display_label.trim();
    if !display_label.is_empty()
        && !matches!(display_label, "动画表情" | "图片" | "表情资源")
    {
        extras.push(format!("name={display_label}"));
    }
    if let Some(path) = path.filter(|path| !path.is_empty()) {
        extras.push(format!("path={path}"));
    }

    if extras.is_empty() {
        "[动画表情]".to_string()
    } else {
        format!("[动画表情]{{{}}}", extras.join(","))
    }
}

fn default_image_label(path: Option<&str>, fallback: &str) -> String {
    match path {
        Some(path) if path.starts_with("FaceStore:") => "表情资源".to_string(),
        Some(path) if path.starts_with("UserDataImage:") => "图片".to_string(),
        _ => fallback.to_string(),
    }
}

fn collect_voice_part(payload: &[u8], info: Option<&InfoMap>) -> Option<VoicePart> {
    let mut ptr = 0;
    let mut part = VoicePart {
        preview: info
            .and_then(|value| value.text("MSGBOX_bsPreviewMsgText"))
            .unwrap_or("[语音消息]")
            .to_string(),
        ..VoicePart::default()
    };

    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x00 | 0x03 => {
                if let Some(nested) = parse_nested_td_payload(data) {
                    part.preview = nested
                        .text("MSGBOX_bsPreviewMsgText")
                        .or_else(|| nested.text("bsAbstractText"))
                        .unwrap_or(&part.preview)
                        .to_string();
                    part.path = part.path.or_else(|| nested.textish("FilePath"));
                    part.full_path = part.full_path.or_else(|| nested.textish("bsFullPath"));
                    part.file_key = part.file_key.or_else(|| nested.textish("bufFileKey"));
                    part.audio_format = part.audio_format.or_else(|| nested.u32("dwAudioFormat"));
                    part.chat_type = part.chat_type.or_else(|| nested.u32("nChatType"));
                    if nested.u32("bEnableAutoTrans") == Some(1) {
                        part.auto_trans_flags.push("auto_trans=1".to_string());
                    }
                    if nested.u32("bEnabledTrans") == Some(1) {
                        part.auto_trans_flags.push("enabled_trans=1".to_string());
                    }
                    if nested.u32("bMsgConvertText") == Some(1) {
                        part.auto_trans_flags.push("convert_text=1".to_string());
                    }
                    part.transcribed_text = part.transcribed_text.or_else(|| {
                        nested
                            .text("strText")
                            .map(str::trim)
                            .filter(|text| !text.is_empty())
                            .map(str::to_string)
                    });
                }
            }
            0x01 => part.hash = Some(hex_string(data)),
            0x02 => part.app = decode_textish_raw(data),
            _ => {}
        }
        ptr += len;
    }

    if let Some(info) = info {
        part.md5 = info.text("bsGroupAudioMd5").map(str::to_string);
        part.file_id = info.u32("dwGroupFileID");
        if part.transcribed_text.is_none() {
            part.transcribed_text = info
                .text("strText")
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string);
        }
    }

    if part.path.is_none() {
        part.path = part.full_path.clone();
    }
    if part.full_path.is_none() {
        part.full_path = part.path.clone();
    }

    Some(part)
}

fn collect_voice_part_compact(payload: &[u8], info: Option<&InfoMap>) -> Option<String> {
    let mut ptr = 0;
    let mut preview = info
        .and_then(|value| value.text("MSGBOX_bsPreviewMsgText"))
        .unwrap_or("[语音消息]")
        .to_string();
    let mut path = None;
    let mut full_path = None;

    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        match inner_type {
            0x00 | 0x03 => {
                if let Some(nested) = parse_nested_td_payload(data) {
                    preview = nested
                        .text("MSGBOX_bsPreviewMsgText")
                        .or_else(|| nested.text("bsAbstractText"))
                        .unwrap_or(&preview)
                        .to_string();
                    if path.is_none() {
                        path = nested.textish("FilePath");
                    }
                    if full_path.is_none() {
                        full_path = nested.textish("bsFullPath");
                    }
                }
            }
            _ => {}
        }
        ptr += len;
    }

    let path = path.or(full_path);
    if let Some(path) = path.as_deref().filter(|path| !path.is_empty()) {
        Some(format!("[语音] path={path}"))
    } else {
        Some(preview)
    }
}

fn collect_video_part(info: &InfoMap) -> Option<VideoPart> {
    let path = info.textish("strVideoFilePath");
    let thumb_path = info.textish("strVideoThumbPath");
    let file_name = info
        .textish("bytes_file_name")
        .and_then(|name| {
            let candidate = if name.contains(['\\', '/']) {
                file_name_from_path(&name)
            } else {
                name
            };
            looks_like_filename(&candidate).then_some(candidate)
        })
        .or_else(|| {
            path.as_deref()
                .map(file_name_from_path)
                .filter(|name| !name.is_empty())
        });

    let reserve_hints = info
        .raw("bytes_pb_reserve")
        .map(parse_video_reserve_hints)
        .unwrap_or_default();
    let part = VideoPart {
        title: file_name
            .map(|name| format!("视频\"{name}\""))
            .unwrap_or_else(|| "[视频]".to_string()),
        path,
        thumb_path,
        size_bytes: info.u32("uint32_file_size"),
        md5: info.raw("bytes_file_md5").map(hex_string),
        thumb_size: match (
            info.u32("uint32_thumb_width"),
            info.u32("uint32_thumb_height"),
        ) {
            (Some(width), Some(height)) => Some((width, height)),
            _ => None,
        },
        thumb_file_size: info.u32("uint32_thumb_file_size"),
        source: info.textish("bytes_source"),
        file_time: info.u32("uint32_file_time"),
        file_format: info.u32("uint32_file_format"),
        video_attr: info.u32("uint32_video_attr"),
        reserve_hints,
    };

    if part.title == "[视频]"
        && part.path.is_none()
        && part.thumb_path.is_none()
        && part.size_bytes.is_none()
        && part.md5.is_none()
        && part.thumb_size.is_none()
        && part.thumb_file_size.is_none()
        && part.source.is_none()
        && part.file_time.is_none()
        && part.file_format.is_none()
        && part.video_attr.is_none()
        && part.reserve_hints.is_empty()
    {
        None
    } else {
        Some(part)
    }
}

fn collect_video_part_compact(info: &InfoMap) -> Option<String> {
    let path = info.textish("strVideoFilePath");
    let thumb_path = info.textish("strVideoThumbPath");
    let file_name = info
        .textish("bytes_file_name")
        .and_then(|name| {
            let candidate = if name.contains(['\\', '/']) {
                file_name_from_path(&name)
            } else {
                name
            };
            looks_like_filename(&candidate).then_some(candidate)
        })
        .or_else(|| {
            path.as_deref()
                .map(file_name_from_path)
                .filter(|name| !name.is_empty())
        });

    let title = file_name
        .map(|name| format!("视频\"{name}\""))
        .unwrap_or_else(|| "[视频]".to_string());

    let mut extras = Vec::new();
    if let Some(path) = path.as_deref().filter(|path| !path.is_empty()) {
        extras.push(format!("path={path}"));
    }
    if let Some(thumb_path) = thumb_path
        .as_deref()
        .filter(|thumb_path| !thumb_path.is_empty())
    {
        extras.push(format!("thumb={thumb_path}"));
    }

    if title == "[视频]" && extras.is_empty() {
        None
    } else if extras.is_empty() {
        Some(title)
    } else {
        Some(format!("{title} {}", extras.join(" ")))
    }
}
fn parse_video_reserve_hints(data: &[u8]) -> Vec<String> {
    select_text_hints(extract_text_candidates(data), 3)
}

fn collect_share_part(payload: &[u8]) -> Option<SharePart> {
    let raw = extract_embedded_card_payload(payload)?;
    if raw.trim_start().starts_with('<') {
        return Some(collect_xml_share_part(&raw));
    }
    if raw.trim_start().starts_with('{') {
        return Some(collect_json_share_part(&raw));
    }
    Some(SharePart {
        brief: Some("[分享]".to_string()),
        summary: Some(raw),
        ..SharePart::default()
    })
}

fn extract_embedded_card_payload(payload: &[u8]) -> Option<String> {
    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if inner_type == 0x01 && !data.is_empty() {
            let raw = if data[0] > 0 {
                inflate_to_string(&data[1..])
                    .unwrap_or_else(|_| String::from_utf8_lossy(&data[1..]).into_owned())
            } else {
                String::from_utf8_lossy(&data[1..]).into_owned()
            };
            let cleaned = raw.trim_matches('\0').trim().to_string();
            if !cleaned.is_empty() {
                return Some(cleaned);
            }
        }
        ptr += len;
    }
    None
}

fn collect_xml_share_part(xml: &str) -> SharePart {
    SharePart {
        brief: extract_xml_attr(xml, "msg", "brief"),
        title: extract_tag_text(xml, "title"),
        summary: extract_tag_text(xml, "summary"),
        source_name: extract_xml_attr(xml, "source", "name"),
        url: extract_xml_attr(xml, "msg", "url").or_else(|| extract_xml_attr(xml, "source", "url")),
        app_hint: extract_xml_attr(xml, "app", "appname")
            .or_else(|| extract_xml_attr(xml, "msg", "serviceID")),
    }
}

fn collect_json_share_part(raw_json: &str) -> SharePart {
    let value = serde_json::from_str::<JsonValue>(raw_json).ok();
    SharePart {
        brief: json_lookup_first(value.as_ref(), &["prompt", "brief", "desc"]).map(str::to_string),
        title: json_lookup_first(value.as_ref(), &["title", "prompt"]).map(str::to_string),
        summary: json_lookup_first(value.as_ref(), &["desc", "summary", "text"])
            .map(str::to_string),
        source_name: json_lookup_first(value.as_ref(), &["sourceName", "appName", "app"])
            .map(str::to_string),
        url: json_lookup_first(value.as_ref(), &["jumpUrl", "url", "qqdocurl"]).map(str::to_string),
        app_hint: json_lookup_first(value.as_ref(), &["app", "appName"]).map(str::to_string),
    }
}

fn json_lookup_first<'a>(value: Option<&'a JsonValue>, keys: &[&str]) -> Option<&'a str> {
    let value = value?;
    for key in keys {
        if let Some(found) = json_lookup(value, key) {
            return Some(found);
        }
    }
    None
}

fn json_lookup<'a>(value: &'a JsonValue, key: &str) -> Option<&'a str> {
    match value {
        JsonValue::Object(map) => {
            if let Some(found) = map.get(key).and_then(JsonValue::as_str) {
                return Some(found);
            }
            for value in map.values() {
                if let Some(found) = json_lookup(value, key) {
                    return Some(found);
                }
            }
            None
        }
        JsonValue::Array(values) => values.iter().find_map(|value| json_lookup(value, key)),
        _ => None,
    }
}

fn collect_file_part(payload: &[u8], info: Option<&InfoMap>) -> Option<FileInfo> {
    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if inner_type == 0x01 {
            let mut file_info = parse_file_proto(data);
            enrich_file_info_from_proto(data, &mut file_info, 0);
            patch_file_info_from_info(&mut file_info, info);
            if file_info.filename.is_empty()
                && file_info.path.is_empty()
                && file_info.md5.is_empty()
            {
                file_info.hints.extend(extract_text_candidates(data));
            }
            return Some(file_info);
        }
        ptr += len;
    }
    if let Some(info) = info {
        if let Some(path) = info.text("strFileSelect") {
            let mut file_info = FileInfo::default();
            file_info.path = path.to_string();
            file_info.filename = file_name_from_path(path);
            return Some(file_info);
        }
    }
    None
}

fn collect_file_part_compact(payload: &[u8], info: Option<&InfoMap>) -> Option<String> {
    if let Some(info) = info {
        if let Some(path) = info.text("strFileSelect") {
            let file_name = file_name_from_path(path);
            if !path.is_empty() {
                return Some(format!("[文件] path={path}"));
            }
            if !file_name.is_empty() {
                return Some(format!("[文件] name={file_name}"));
            }
        }
    }

    let mut ptr = 0;
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if inner_type == 0x01 {
            let mut file_info = parse_file_proto(data);
            patch_file_info_from_info(&mut file_info, info);
            return Some(format_file_info_compact(&file_info));
        }
        ptr += len;
    }
    None
}

fn enrich_file_info_from_proto(data: &[u8], info: &mut FileInfo, depth: usize) {
    if depth > 4 {
        return;
    }
    let mut ptr = 0;
    while let Ok((field, value)) = next_proto_field(data, &mut ptr) {
        match value {
            ProtoValue::Len(bytes) => {
                let text = String::from_utf8_lossy(bytes)
                    .trim_matches('\0')
                    .trim()
                    .to_string();
                if looks_like_filename(&text) && info.filename.is_empty() {
                    info.filename = text.clone();
                }
                if (text.contains('\\') || text.contains('/')) && info.path.is_empty() {
                    info.path = text.clone();
                }
                if looks_like_md5(&text) && info.md5.is_empty() {
                    info.md5 = text.clone();
                }
                if looks_like_json(&text) && info.extra_json.is_empty() {
                    info.extra_json = normalize_json_text(&text).unwrap_or(text.clone());
                }
                if field == 7 && info.extra_json.is_empty() && looks_like_json(&text) {
                    info.extra_json = normalize_json_text(&text).unwrap_or(text.clone());
                }
                if !text.is_empty() && !looks_like_filename(&text) && !looks_like_json(&text) {
                    let cleaned = normalize_text_hint(text);
                    if !cleaned.is_empty() && !info.hints.contains(&cleaned) {
                        info.hints.push(cleaned);
                    }
                }
                enrich_file_info_from_proto(bytes, info, depth + 1);
            }
            ProtoValue::Varint(size) => {
                if info.size_bytes.is_none() && size > 0 && size < u32::MAX as u64 * 16 {
                    info.size_bytes = Some(size);
                }
            }
            _ => {}
        }
        if ptr >= data.len() {
            break;
        }
    }
}

fn collect_forward_record(payload: &[u8]) -> Option<ForwardRecordPart> {
    let info = parse_nested_td_payload(payload)?;
    let title = info
        .text("title")
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    let brief = info
        .text("brief")
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    let count = info
        .u32("dwMsgPackListSize")
        .or_else(|| info.u32("totalMsg"))
        .filter(|count| *count > 0);

    let mut unresolved_blob_meta = Vec::new();
    for key in [
        "bsResId",
        "bsFileName",
        "titleList",
        "buffMsgPackListStream",
        "arrPackets",
        "arrSeqList",
    ] {
        if info.contains_key(key) {
            unresolved_blob_meta.push(key.to_string());
        }
    }

    let mut items = Vec::new();
    for candidate in collect_forward_candidates(&info, payload) {
        if let Some(item) = parse_forward_item_candidate(&candidate) {
            if !items.iter().any(|existing: &ForwardItem| {
                existing.sender == item.sender && existing.summary == item.summary
            }) {
                items.push(item);
            }
        }
    }

    Some(ForwardRecordPart {
        title,
        brief,
        count,
        items,
        unresolved_blob_meta,
    })
}

fn collect_forward_candidates(info: &InfoMap, payload: &[u8]) -> Vec<String> {
    let mut candidates = Vec::new();
    for text in binary_payload_hints(payload) {
        let text = normalize_text_hint(text);
        if !text.is_empty() && !candidates.contains(&text) {
            candidates.push(text);
        }
    }
    if let Some(file_name) = info
        .textish("bsFileName")
        .map(|text| normalize_text_hint(text))
        .filter(|text| !text.is_empty())
    {
        if !candidates.contains(&file_name) {
            candidates.push(file_name);
        }
    }
    candidates
}

fn parse_forward_item_candidate(candidate: &str) -> Option<ForwardItem> {
    let cleaned = normalize_text_hint(candidate.to_string());
    if cleaned.is_empty() {
        return None;
    }

    let (sender, summary) = split_sender_hint(&cleaned);
    if !looks_like_filename(summary) && !is_plausible_textish(summary) {
        return None;
    }
    let kind = classify_forward_item_kind(summary);
    Some(ForwardItem {
        sender: sender.map(str::to_string),
        kind,
        summary: summary.to_string(),
    })
}

fn split_sender_hint(text: &str) -> (Option<&str>, &str) {
    for sep in [": ", "："] {
        if let Some((sender, rest)) = text.split_once(sep) {
            let sender = sender.trim();
            let rest = rest.trim();
            if !sender.is_empty() && sender.chars().count() <= 24 && !rest.is_empty() {
                return (Some(sender), rest);
            }
        }
    }
    (None, text.trim())
}

fn classify_forward_item_kind(text: &str) -> ForwardItemKind {
    if text.contains("[文件]") || looks_like_filename(text) {
        ForwardItemKind::File
    } else if text.contains("[图片]") || text.contains("动画表情") || text.contains("[表情资源]")
    {
        ForwardItemKind::Image
    } else if text.contains("[语音") {
        ForwardItemKind::Voice
    } else if text.contains("[视频") {
        ForwardItemKind::Video
    } else if text.contains("[分享]") {
        ForwardItemKind::Share
    } else if text.is_empty() {
        ForwardItemKind::Unknown
    } else {
        ForwardItemKind::Text
    }
}

fn render_forward_record(record: &ForwardRecordPart) -> String {
    let mut out = format_forward_brief(record.brief.as_deref(), record.title.as_deref());
    if let Some(count) = record.count {
        out.push_str(&format!("({count}条)"));
    }
    if let Some(item) = record.items.first() {
        out.push_str(" - ");
        if let Some(sender) = item.sender.as_deref() {
            out.push_str(sender);
            out.push_str(": ");
        }
        out.push_str(&item.summary);
    }
    out
}

#[allow(dead_code)]
fn decode_binary_hint_payload(payload: &[u8], label: &str) -> String {
    let hints = binary_payload_hints(payload);
    if hints.is_empty() {
        label.to_string()
    } else {
        format!("{label} {}", hints.join(" | "))
    }
}

fn binary_payload_hints(payload: &[u8]) -> Vec<String> {
    let mut ptr = 0;
    let mut candidates = Vec::new();
    while ptr + 3 <= payload.len() {
        let inner_type = payload[ptr];
        ptr += 1;
        let len = u16::from_le_bytes([payload[ptr], payload[ptr + 1]]) as usize;
        ptr += 2;
        if ptr + len > payload.len() {
            break;
        }
        let data = &payload[ptr..ptr + len];
        if matches!(inner_type, 0x00 | 0x01 | 0x02) {
            candidates.extend(extract_text_candidates(data));
        }
        ptr += len;
    }

    if candidates.is_empty() {
        candidates.extend(extract_text_candidates(payload));
    }

    select_text_hints(candidates, 3)
}

fn extract_text_candidates(data: &[u8]) -> Vec<String> {
    let mut out = extract_utf16_strings_endian(data, false);
    out.extend(extract_utf16_strings_endian(data, true));
    out.extend(extract_ascii_strings(data));
    out
}

fn extract_ascii_strings(data: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for &byte in data {
        let ch = byte as char;
        if ch.is_ascii_graphic() || ch == ' ' {
            current.push(ch);
        } else if current.chars().count() >= 4 {
            out.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if current.chars().count() >= 4 {
        out.push(current);
    }
    out
}

fn decode_utf16le(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() / 2);
    append_utf16le_to_string(&mut out, data);
    out
}

fn append_utf16le_to_string(out: &mut String, data: &[u8]) {
    let data = if data.len() % 2 == 1 {
        &data[..data.len() - 1]
    } else {
        data
    };
    for ch in std::char::decode_utf16(
        data.chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])),
    ) {
        out.push(ch.unwrap_or(char::REPLACEMENT_CHARACTER));
    }
}

fn extract_utf16_strings_endian(data: &[u8], big_endian: bool) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for chunk in data.chunks_exact(2) {
        let code = if big_endian {
            u16::from_be_bytes([chunk[0], chunk[1]])
        } else {
            u16::from_le_bytes([chunk[0], chunk[1]])
        };
        let ch = char::from_u32(code as u32).unwrap_or_default();
        if is_text_hint_char(code, ch) {
            current.push(ch);
        } else if current.chars().count() >= 4 {
            out.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if current.chars().count() >= 4 {
        out.push(current);
    }
    out
}

fn extract_utf16le_strings(data: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for chunk in data.chunks_exact(2) {
        let code = u16::from_le_bytes([chunk[0], chunk[1]]);
        if (32..=126).contains(&code) || (0x4e00..=0x9fff).contains(&code) {
            current.push(char::from_u32(code as u32).unwrap_or_default());
        } else if current.len() >= 4 {
            out.push(std::mem::take(&mut current));
        } else {
            current.clear();
        }
    }
    if current.len() >= 4 {
        out.push(current);
    }
    out
}

fn select_text_hints(candidates: Vec<String>, limit: usize) -> Vec<String> {
    let mut scored: Vec<(i32, String)> = candidates
        .into_iter()
        .map(normalize_text_hint)
        .filter(|text| !text.is_empty())
        .map(|text| (text_hint_score(&text), text))
        .filter(|(score, _)| *score >= 30)
        .collect();

    scored.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.chars().count().cmp(&left.1.chars().count()))
            .then_with(|| left.1.cmp(&right.1))
    });

    let mut out = Vec::new();
    for (_, text) in scored {
        if out
            .iter()
            .any(|existing: &String| existing.contains(&text) || text.contains(existing))
        {
            continue;
        }
        out.push(text);
        if out.len() >= limit {
            break;
        }
    }

    out
}

fn normalize_text_hint(text: String) -> String {
    clean_text_hint(text.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn clean_text_hint(text: String) -> String {
    let text = text.trim().to_string();
    let lower = text.to_ascii_lowercase();
    let suffixes = [
        ".txt", ".zip", ".rar", ".7z", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mkv",
        ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".apk",
    ];

    let mut cutoff: Option<usize> = None;
    for suffix in suffixes {
        if let Some(pos) = lower.find(suffix) {
            let end = pos + suffix.len();
            cutoff = Some(cutoff.map(|current| current.max(end)).unwrap_or(end));
        }
    }

    if let Some(end) = cutoff {
        return text[..end].to_string();
    }

    text
}

fn text_hint_score(text: &str) -> i32 {
    let mut score = text.chars().count().min(60) as i32;

    if text.contains(':') || text.contains('：') {
        score += 20;
    }
    if text.contains('[') || text.contains(']') {
        score += 15;
    }
    if looks_like_filename(text) {
        score += 18;
    }

    let lower = text.to_ascii_lowercase();
    if lower.contains("http") || lower.contains("qq") {
        score += 8;
    }

    let ascii_alnum_count = text.chars().filter(|ch| ch.is_ascii_alphanumeric()).count();
    if ascii_alnum_count >= 4 {
        score += 8;
    }

    let common_hits = common_cjk_hint_count(text);
    score += (common_hits as i32 * 2).min(30);

    let cjk_count = text.chars().filter(|ch| is_cjk(*ch)).count();
    let has_strong_marker = text.contains(':')
        || text.contains('：')
        || looks_like_filename(text)
        || lower.contains("http")
        || lower.contains("qq")
        || ascii_alnum_count >= 4;

    if cjk_count >= 10 && common_hits <= 1 && !has_strong_marker {
        score -= 100;
    } else if cjk_count >= 4 && common_hits == 0 && !has_strong_marker {
        score -= 55;
    }

    if text.chars().count() <= 5 && common_hits == 0 && !has_strong_marker {
        score -= 25;
    }

    score
}

fn common_cjk_hint_count(text: &str) -> usize {
    const COMMON_HINTS: &str = "的一了是不在人有我这你他她它们个就和到也很还把让给吧吗呢啊哦呀着与及后前中上下载打开文件图片视频语音聊天记录分享程序游戏模式练习群聊这个那个国内其他几个做教程站搜两个放到路径下小程序哔哩";
    text.chars().filter(|ch| COMMON_HINTS.contains(*ch)).count()
}

fn is_cjk(ch: char) -> bool {
    ('\u{4e00}'..='\u{9fff}').contains(&ch)
}

fn is_text_hint_char(code: u16, ch: char) -> bool {
    (32..=126).contains(&code)
        || is_cjk(ch)
        || matches!(
            ch,
            '[' | ']'
                | '('
                | ')'
                | '{'
                | '}'
                | '-'
                | '_'
                | '.'
                | '/'
                | '\\'
                | ':'
                | '：'
                | '，'
                | '。'
                | '！'
                | '？'
                | '“'
                | '”'
                | '‘'
                | '’'
                | '《'
                | '》'
                | '@'
                | '#'
                | '&'
                | '+'
                | '='
                | ' '
        )
}

fn looks_like_filename(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        ".txt", ".zip", ".rar", ".7z", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mkv",
        ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".apk",
    ]
    .iter()
    .any(|suffix| lower.contains(suffix))
}

fn extract_tag_text(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{tag}>");
    let end_tag = format!("</{tag}>");
    let start = xml.find(&start_tag)? + start_tag.len();
    let end = xml[start..].find(&end_tag)? + start;
    Some(html_unescape(&xml[start..end]))
}

fn extract_xml_attr(xml: &str, tag: &str, attr: &str) -> Option<String> {
    let start = xml.find(&format!("<{tag}"))?;
    let end = xml[start..].find('>')? + start;
    let head = &xml[start..end];

    for quote in ['"', '\''] {
        let pattern = format!("{attr}={quote}");
        if let Some(idx) = head.find(&pattern) {
            let begin = idx + pattern.len();
            if let Some(end_idx) = head[begin..].find(quote) {
                return Some(html_unescape(&head[begin..begin + end_idx]));
            }
        }
    }
    None
}

fn html_unescape(text: &str) -> String {
    text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

enum ProtoValue<'a> {
    Varint(u64),
    Len(&'a [u8]),
    Fixed32(u32),
    Fixed64(u64),
}

fn next_proto_field<'a>(data: &'a [u8], ptr: &mut usize) -> Result<(u64, ProtoValue<'a>)> {
    if *ptr >= data.len() {
        bail!("proto eof");
    }
    let tag = read_varint(data, ptr)?;
    let field = tag >> 3;
    let wire_type = (tag & 0x07) as u8;
    let value = match wire_type {
        0 => ProtoValue::Varint(read_varint(data, ptr)?),
        1 => ProtoValue::Fixed64(read_fixed64_le(data, ptr)?),
        2 => {
            let len = read_varint(data, ptr)? as usize;
            ProtoValue::Len(read_slice(data, ptr, len)?)
        }
        5 => ProtoValue::Fixed32(read_fixed32_le(data, ptr)?),
        other => bail!("unsupported proto wire_type: {other}"),
    };
    Ok((field, value))
}

fn read_varint(data: &[u8], ptr: &mut usize) -> Result<u64> {
    let mut result = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = read_u8(data, ptr)?;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint too long");
        }
    }
}

fn read_fixed32_le(data: &[u8], ptr: &mut usize) -> Result<u32> {
    let bytes = read_slice(data, ptr, 4)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_fixed64_le(data: &[u8], ptr: &mut usize) -> Result<u64> {
    let bytes = read_slice(data, ptr, 8)?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_u8(data: &[u8], ptr: &mut usize) -> Result<u8> {
    if *ptr >= data.len() {
        bail!("read_u8 out of bounds");
    }
    let value = data[*ptr];
    *ptr += 1;
    Ok(value)
}

fn read_u16_le(data: &[u8], ptr: &mut usize) -> Result<u16> {
    let bytes = read_slice(data, ptr, 2)?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_u32_le(data: &[u8], ptr: &mut usize) -> Result<u32> {
    let bytes = read_slice(data, ptr, 4)?;
    Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_slice<'a>(data: &'a [u8], ptr: &mut usize, len: usize) -> Result<&'a [u8]> {
    if *ptr + len > data.len() {
        return Err(anyhow!(
            "read_slice out of bounds: ptr={}, len={}, total={}",
            *ptr,
            len,
            data.len()
        ));
    }
    let slice = &data[*ptr..*ptr + len];
    *ptr += len;
    Ok(slice)
}

fn hex_string(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[allow(dead_code)]
fn _touch_proto_values(value: &ProtoValue<'_>) {
    match value {
        ProtoValue::Varint(_) | ProtoValue::Len(_) => {}
        ProtoValue::Fixed32(value) => {
            let _ = *value;
        }
        ProtoValue::Fixed64(value) => {
            let _ = *value;
        }
    }
}
