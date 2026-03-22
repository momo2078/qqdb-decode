import argparse
import binascii
import csv
import sqlite3
import struct
from dataclasses import dataclass, field


MSG_TEXT = 1
MSG_FACE = 2
MSG_GROUP_IMAGE = 3
MSG_PRIVATE_IMAGE = 6
MSG_VOICE = 7
MSG_NICKNAME = 18
MSG_VIDEO = 26


def qident(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


class Buffer:
    def __init__(self, data: bytes):
        self.data = data
        self.off = 0

    def empty(self) -> bool:
        return self.off >= len(self.data)

    def remaining(self) -> int:
        return len(self.data) - self.off

    def read(self, n: int) -> bytes:
        if self.off + n > len(self.data):
            raise ValueError("buffer underflow")
        out = self.data[self.off : self.off + n]
        self.off += n
        return out

    def skip(self, n: int) -> None:
        self.read(n)

    def byte(self) -> int:
        return self.read(1)[0]

    def uint16(self) -> int:
        return struct.unpack("<H", self.read(2))[0]

    def uint32(self) -> int:
        return struct.unpack("<I", self.read(4))[0]

    def tlv(self) -> tuple[int, int, bytes]:
        t = self.byte()
        l = self.uint16()
        v = self.read(l)
        return t, l, v


def decode_utf16le(data: bytes) -> str:
    if not data:
        return ""
    if len(data) % 2 == 1:
        data = data[:-1]
    return data.decode("utf-16le", errors="ignore")


@dataclass
class Header:
    time: int = 0
    rand: int = 0
    color: int = 0
    font_size: int = 0
    font_style: int = 0
    charset: int = 0
    font_family: int = 0
    font_name: str = ""


@dataclass
class ParsedMessage:
    header: Header = field(default_factory=Header)
    sender_nickname: str = ""
    elements: list[str] = field(default_factory=list)
    unknown_outer_types: list[int] = field(default_factory=list)

    @property
    def text(self) -> str:
        return "".join(self.elements)


def decode_nickname(data: bytes) -> str:
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t in (1, 2):
            return decode_utf16le(v)
    return ""


def decode_at(data: bytes) -> str | None:
    if len(data) < 11:
        return None
    if len(data) >= 7 and data[6] == 1:
        return "@全体成员"
    if len(data) >= 11:
        uin = struct.unpack(">I", data[7:11])[0]
        return f"@{uin}"
    return None


def decode_text_payload(data: bytes) -> list[str]:
    out: list[str] = []
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == 1:
            out.append(decode_utf16le(v))
        elif t == 6:
            at = decode_at(v)
            if at:
                out.append(at)
        elif t in (2, 3):
            # 链接/平台等结构，先不拼到正文里，避免污染纯文本结果
            continue
        else:
            continue
    return out


def decode_face_payload(data: bytes) -> str | None:
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == 1:
            face_id = 0
            for b in v:
                face_id = (face_id << 8) | b
            return f"[表情:{face_id}]"
    return "[表情]"


def decode_image_payload(data: bytes, label: str) -> str:
    path = ""
    hash_bytes = b""
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == 1:
            hash_bytes = v
        elif t == 2:
            path = decode_utf16le(v)
    extra = []
    if path:
        extra.append(f"path={path}")
    if hash_bytes:
        extra.append(f"hash={binascii.hexlify(hash_bytes).decode('ascii')}")
    return f"[{label}{',' + ','.join(extra) if extra else ''}]"


def decode_voice_payload(data: bytes) -> str:
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == 1:
            return f"[语音:hash={binascii.hexlify(v).decode('ascii')}]"
    return "[语音]"


def decode_video_payload(data: bytes) -> str:
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == 1:
            if len(v) >= 260:
                h = bytearray(v[244:260])
                for i in range(len(h)):
                    h[i] ^= 0xEF
                return f"[视频:hash={binascii.hexlify(h).decode('ascii')}]"
            return f"[视频:raw={binascii.hexlify(v[:32]).decode('ascii')}]"
    return "[视频]"


def unpack_message(data: bytes) -> ParsedMessage:
    if len(data) < 26:
        raise ValueError("message too short")

    msg = ParsedMessage()
    buf = Buffer(data)
    buf.skip(8)
    msg.header.time = buf.uint32()
    msg.header.rand = buf.uint32()
    msg.header.color = buf.uint32()
    msg.header.font_size = buf.byte()
    msg.header.font_style = buf.byte()
    msg.header.charset = buf.byte()
    msg.header.font_family = buf.byte()
    font_name = buf.read(buf.uint16())
    msg.header.font_name = decode_utf16le(font_name)
    buf.skip(2)

    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == MSG_TEXT:
            msg.elements.extend(decode_text_payload(v))
        elif t == MSG_FACE:
            elem = decode_face_payload(v)
            if elem:
                msg.elements.append(elem)
        elif t == MSG_GROUP_IMAGE:
            msg.elements.append(decode_image_payload(v, "群图片"))
        elif t == MSG_PRIVATE_IMAGE:
            msg.elements.append(decode_image_payload(v, "私聊图片"))
        elif t == MSG_VOICE:
            msg.elements.append(decode_voice_payload(v))
        elif t == MSG_VIDEO:
            msg.elements.append(decode_video_payload(v))
        elif t == MSG_NICKNAME:
            msg.sender_nickname = decode_nickname(v)
        else:
            msg.unknown_outer_types.append(t)
    return msg


def iter_message_tables(conn: sqlite3.Connection, table_like: str | None = None) -> list[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = []
    for (name,) in cur.fetchall():
        if "$" in name:
            continue
        try:
            cur.execute(f"PRAGMA table_info({qident(name)})")
            cols = {row[1] for row in cur.fetchall()}
        except sqlite3.Error:
            continue
        if "MsgContent" in cols:
            if table_like and table_like not in name:
                continue
            tables.append(name)
    return tables


def ensure_decoded_column(conn: sqlite3.Connection, table_name: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({qident(table_name)})")
    cols = {row[1] for row in cur.fetchall()}
    if "DecodedMsg" not in cols:
        cur.execute(f"ALTER TABLE {qident(table_name)} ADD COLUMN DecodedMsg TEXT")
        conn.commit()


def preview_to_csv(
    db_path: str, output_path: str, limit_per_table: int, table_like: str | None
) -> None:
    conn = sqlite3.connect(db_path)
    tables = iter_message_tables(conn, table_like)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "table",
                "rowid",
                "time",
                "rand",
                "sender_uin",
                "sender_nickname",
                "decoded_msg",
                "unknown_outer_types",
            ]
        )
        cur = conn.cursor()
        for table_name in tables:
            cur.execute(
                f"SELECT rowid, Time, Rand, SenderUin, MsgContent FROM {qident(table_name)} LIMIT ?",
                (limit_per_table,),
            )
            for rowid, time, rand, sender_uin, msg_content in cur.fetchall():
                try:
                    msg = unpack_message(msg_content)
                    decoded = msg.text
                    unknown = ",".join(map(str, msg.unknown_outer_types))
                    nickname = msg.sender_nickname
                except Exception as e:
                    decoded = f"<decode error: {e}>"
                    unknown = ""
                    nickname = ""
                writer.writerow(
                    [table_name, rowid, time, rand, sender_uin, nickname, decoded, unknown]
                )
    conn.close()


def update_db(
    db_path: str, limit_per_table: int | None, table_like: str | None
) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tables = iter_message_tables(conn, table_like)
    for table_name in tables:
        ensure_decoded_column(conn, table_name)
        print(f"开始写入 {table_name}")
        cur.execute(
            f"SELECT rowid, MsgContent FROM {qident(table_name)}"
            + (" LIMIT ?" if limit_per_table is not None else ""),
            (() if limit_per_table is None else (limit_per_table,)),
        )
        rows = cur.fetchall()
        counter = 0
        for rowid, msg_content in rows:
            try:
                decoded = unpack_message(msg_content).text
            except Exception as e:
                decoded = f"<decode error: {e}>"
            cur.execute(
                f"UPDATE {qident(table_name)} SET DecodedMsg = ? WHERE rowid = ?",
                (decoded, rowid),
            )
            counter += 1
            if counter % 1000 == 0:
                conn.commit()
                print(f"  已处理 {counter} 条")
        conn.commit()
        print(f"完成 {table_name}: {counter} 条")
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="预览/写回 QQ MsgContent 解码结果")
    parser.add_argument("db_path", help="解密后的 sqlite 数据库路径")
    parser.add_argument(
        "--limit-per-table",
        type=int,
        default=20,
        help="每个表预览多少条，默认 20",
    )
    parser.add_argument(
        "--output",
        default="decoded_preview.csv",
        help="预览 CSV 输出文件，默认 decoded_preview.csv",
    )
    parser.add_argument(
        "--update-db",
        action="store_true",
        help="把解码结果写回数据库的 DecodedMsg 列",
    )
    parser.add_argument(
        "--table-like",
        default=None,
        help="只处理表名里包含该字符串的表",
    )
    args = parser.parse_args()

    if args.update_db:
        update_db(args.db_path, args.limit_per_table, args.table_like)
    else:
        preview_to_csv(args.db_path, args.output, args.limit_per_table, args.table_like)
        print(f"预览已输出到 {args.output}")


if __name__ == "__main__":
    main()
