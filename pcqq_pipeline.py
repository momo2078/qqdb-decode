"""
PCQQ 一键解密流水线

将密钥获取、数据库解密、消息解码三个步骤合并为一个脚本。

  Phase 1: Frida hook QQ 进程，捕获所有数据库密钥 → keys.json
  Phase 2: 调用 pcqq_batch_decrypt.exe 逐个 rekey 解密 → *.decrypted.db
  Phase 3: 解码 Msg3.0.decrypted.db 中的 MsgContent 字段 → DecodedMsg 列

用法:
  # 完整流程（需要 QQ 打开但未登录）
  python pcqq_pipeline.py <QQ数据库目录>

  # 跳过密钥获取（已有 keys.json）
  python pcqq_pipeline.py <QQ数据库目录> --skip-keys

  # 只做消息解码（已有解密数据库）
  python pcqq_pipeline.py --skip-keys --skip-decrypt --decrypt-dir <解密后数据库目录>

依赖:
  - frida, psutil（Phase 1 需要）
  - pcqq_batch_decrypt.exe（Phase 2，放在脚本同目录）
  - qqdb-decode.exe（Phase 3 可选，放在脚本同目录；没有则用内嵌 Python 解码器）
"""

import argparse
import binascii
import json
import os
import shutil
import sqlite3
import struct
import subprocess
import sys
import time
from pathlib import Path


def fmt_duration(seconds: float) -> str:
    """格式化耗时为人类可读字符串"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{int(m)}m{s:.1f}s"
    h, m = divmod(m, 60)
    return f"{int(h)}h{int(m)}m{s:.0f}s"


SCRIPT_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------
#  Phase 1: 获取密钥
# ---------------------------------------------------------------

HOOK_SCRIPT = """
function buf2hex(buffer) {
  const byteArray = new Uint8Array(buffer);
  const hexParts = [];
  for(let i = 0; i < byteArray.length; i++) {
    const hex = byteArray[i].toString(16);
    const paddedHex = ('00' + hex).slice(-2);
    hexParts.push(paddedHex);
  }
  return '0x' + hexParts.join(', 0x');
}

function buf2hexCompact(buffer) {
  const byteArray = new Uint8Array(buffer);
  const hexParts = [];
  for(let i = 0; i < byteArray.length; i++) {
    const hex = byteArray[i].toString(16);
    const paddedHex = ('00' + hex).slice(-2);
    hexParts.push(paddedHex);
  }
  return hexParts.join('');
}

const kernel_util = Module.load('KernelUtil.dll');
function single_function(pattern) {
    pattern = pattern.replaceAll("##", "").replaceAll(" ", "").toLowerCase().replace(/\\s/g,'').replace(/(.{2})/g,"$1 ");
    var akey_function_list = Memory.scanSync(kernel_util.base, kernel_util.size, pattern);
    if (akey_function_list.length > 1) {
        send(JSON.stringify({"type": "error", "msg": "pattern FOUND MULTI: " + pattern}));
        return null;
    }
    if (akey_function_list.length == 0) {
        send(JSON.stringify({"type": "error", "msg": "pattern NOT FOUND: " + pattern}));
        return null;
    }
    return akey_function_list[0]['address'];
}

const key_function = single_function("55 8b ec 56 6b 75 10 11 83 7d 10 10 74 0d 68 17 02 00 00 e8");
const name_function = single_function("55 8B EC FF 75 0C FF 75 08 E8 B8 D1 02 00 59 59 85");

if (!key_function || !name_function) {
    send(JSON.stringify({"type": "error", "msg": "PATTERN NOT FOUND! 当前QQ版本可能不受支持。"}));
} else {
    var funcName = new NativeFunction(name_function, 'pointer', ['pointer', 'pointer']);

    Interceptor.attach(key_function, {
        onEnter: function (args, state) {
            var dbName = funcName(args[0], NULL).readUtf8String();
            var nKey = args[2].toInt32();
            var keyHex = buf2hexCompact(args[1].readByteArray(nKey));
            var keyDisplay = buf2hex(args[1].readByteArray(nKey));
            var fileName = dbName.replaceAll('/', '\\\\').split('\\\\').pop();

            send(JSON.stringify({
                "type": "key",
                "dbName": dbName,
                "fileName": fileName,
                "nKey": nKey,
                "keyHex": keyHex,
                "keyDisplay": keyDisplay
            }));
        },
        onLeave: function (retval, state) {
        }
    });

    send(JSON.stringify({"type": "ready", "msg": "Hook 已就绪，请在 QQ 窗口中登录..."}));
}
"""


def phase_get_keys(keys_file: str) -> dict:
    """Phase 1: 通过 Frida hook QQ 进程获取所有数据库密钥"""
    print(f"\n{'='*60}")
    print("[Phase 1] 获取密钥")
    print(f"{'='*60}")

    try:
        import frida
        import psutil
    except ImportError:
        print("[ERR] 需要安装 frida 和 psutil:")
        print("   pip install frida frida-tools psutil")
        sys.exit(1)

    # 查找 QQ 进程
    qq_pid = None
    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            if p.name() == "QQ.exe" and len(p.cmdline()) > 1:
                qq_pid = pid
                del p
                break
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if qq_pid is None:
        print("[ERR] QQ 未启动。请先打开 QQ（不要登录）。")
        sys.exit(1)

    print(f"[OK] 找到 QQ 进程 (PID: {qq_pid})")

    session = frida.get_local_device().attach(qq_pid)
    script = session.create_script(HOOK_SCRIPT)

    all_keys = {}

    def on_message(message, data):
        if message["type"] == "send":
            try:
                payload = json.loads(message["payload"])
            except (json.JSONDecodeError, TypeError):
                print(message["payload"])
                return

            msg_type = payload.get("type")
            if msg_type == "error":
                print(f"[ERR] {payload['msg']}")
            elif msg_type == "ready":
                print(f"[OK] {payload['msg']}")
            elif msg_type == "key":
                db_name = payload["dbName"]
                if db_name not in all_keys:
                    print(f"\n[KEY] [{payload['fileName']}]")
                    print(f"   路径: {db_name}")
                    print(f"   密钥长度: {payload['nKey']}")
                    all_keys[db_name] = {
                        "fileName": payload["fileName"],
                        "nKey": payload["nKey"],
                        "keyHex": payload["keyHex"],
                        "keyDisplay": payload["keyDisplay"],
                    }
                    # 实时保存
                    with open(keys_file, "w", encoding="utf-8") as f:
                        json.dump(all_keys, f, indent=2, ensure_ascii=False)
                    print(f"   已保存 (共 {len(all_keys)} 个密钥)")
        elif message["type"] == "error":
            print(f"[ERR] Frida 错误: {message['stack']}")

    def on_destroyed():
        print("\nQQ 进程已退出。")

    script.on("message", on_message)
    script.on("destroyed", on_destroyed)
    script.load()

    print(f"\n{'─'*60}")
    print(
        "Hook 已注入! 请登录账号，打开对话、消息、联系人、收藏、文件管理、频道、空间等获取密钥"
    )
    print(f"{'─'*60}")
    input("\n>>> 密钥捕获完成后，按回车继续 <<<\n")

    # 断开 Frida
    try:
        script.unload()
        session.detach()
    except Exception:
        pass

    print(f"[OK] Phase 1 完成: 共捕获 {len(all_keys)} 个密钥 -> {keys_file}")
    return all_keys, len(all_keys)


# ---------------------------------------------------------------
#  Phase 2: 批量解密
# ---------------------------------------------------------------


def find_qq_bin_dir():
    """查找 QQ 安装目录的 Bin 文件夹"""
    candidates = [
        r"C:\Program Files (x86)\Tencent\QQ\Bin",
        r"C:\Program Files\Tencent\QQ\Bin",
        r"D:\Program Files (x86)\Tencent\QQ\Bin",
        r"D:\Program Files\Tencent\QQ\Bin",
        r"D:\Tencent\QQ\Bin",
        r"C:\Tencent\QQ\Bin",
    ]
    for d in candidates:
        if os.path.isdir(d) and os.path.exists(os.path.join(d, "KernelUtil.dll")):
            return d

    try:
        import winreg

        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\QQ",
        )
        val, _ = winreg.QueryValueEx(key, "UninstallString")
        winreg.CloseKey(key)
        install_dir = os.path.dirname(val.strip('"'))
        bin_dir = os.path.join(install_dir, "Bin")
        if os.path.isdir(bin_dir):
            return bin_dir
    except Exception:
        pass

    return None


def resolve_qq_bin_dir(cli_qq_bin: str | None):
    """解析 QQ Bin 目录，优先使用命令行参数"""
    if cli_qq_bin:
        qq_bin = os.path.abspath(cli_qq_bin)
        if not os.path.isdir(qq_bin):
            print(f"[ERR] 指定的 QQ Bin 目录不存在: {qq_bin}")
            sys.exit(1)
        if not os.path.exists(os.path.join(qq_bin, "KernelUtil.dll")):
            print(f"[ERR] 指定目录里缺少 KernelUtil.dll: {qq_bin}")
            sys.exit(1)
        return qq_bin

    qq_bin = find_qq_bin_dir()
    if not qq_bin:
        print("[ERR] 未找到 QQ 安装目录的 Bin 文件夹")
        print("   可通过 --qq-bin 手动指定")
        sys.exit(1)
    return qq_bin


def build_output_path(src_path, db_dir, output_dir):
    """为解密结果生成唯一输出路径"""
    src_path = os.path.normpath(src_path)
    db_dir = os.path.normpath(db_dir)
    try:
        if os.path.commonpath([src_path, db_dir]) == db_dir:
            rel_path = os.path.relpath(src_path, db_dir)
        else:
            drive, tail = os.path.splitdrive(src_path)
            drive_name = drive.rstrip(":\\/") or "unknown_drive"
            rel_path = os.path.join("_external", drive_name, tail.lstrip("\\/"))
    except ValueError:
        drive, tail = os.path.splitdrive(src_path)
        drive_name = drive.rstrip(":\\/") or "unknown_drive"
        rel_path = os.path.join("_external", drive_name, tail.lstrip("\\/"))

    if rel_path.lower().endswith(".db"):
        rel_path = rel_path[:-3] + ".decrypted.db"
    else:
        rel_path = rel_path + ".decrypted.db"

    return os.path.join(output_dir, rel_path)


def phase_decrypt_all(
    keys_file: str, db_dir: str, output_dir: str, cli_qq_bin: str | None = None
) -> dict:
    """Phase 2: 批量调用 pcqq_batch_decrypt.exe 解密所有数据库"""
    print(f"\n{'='*60}")
    print("[Phase 2] 批量解密")
    print(f"{'='*60}")

    if not os.path.exists(keys_file):
        print(f"[ERR] 密钥文件不存在: {keys_file}")
        print("   请先运行 Phase 1 获取密钥")
        sys.exit(1)

    with open(keys_file, "r", encoding="utf-8") as f:
        all_keys = json.load(f)

    print(f"  加载了 {len(all_keys)} 个密钥")
    print(f"  数据库目录: {db_dir}")
    print(f"  输出目录: {output_dir}")

    # 查找 QQ Bin 目录
    qq_bin = resolve_qq_bin_dir(cli_qq_bin)
    print(f"  QQ Bin: {qq_bin}")

    # 确保 pcqq_batch_decrypt.exe 可用
    exe_name = "pcqq_batch_decrypt.exe"
    exe_in_bin = os.path.join(qq_bin, exe_name)
    exe_local = SCRIPT_DIR / exe_name

    if not os.path.exists(exe_in_bin):
        if exe_local.exists():
            print(f"  复制 {exe_name} -> {qq_bin}")
            shutil.copy2(str(exe_local), exe_in_bin)
        else:
            print(f"[ERR] 未找到 {exe_name}，请放在 {SCRIPT_DIR} 下")
            sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    stats = {"success": 0, "failed": 0, "skipped": 0}
    total = len(all_keys)
    idx = 0
    phase2_start = time.perf_counter()

    for db_full_path, key_info in all_keys.items():
        idx += 1
        file_name = key_info["fileName"]
        key_hex = key_info["keyHex"]
        n_key = key_info["nKey"]
        elapsed = time.perf_counter() - phase2_start

        print(f"\n{'─'*60}")
        print(f"[{idx}/{total}] {file_name}  (已用时 {fmt_duration(elapsed)})")
        print(f"   原始路径: {db_full_path}")

        if n_key != 16:
            print(f"   [WARN] 密钥长度 {n_key} != 16，跳过")
            stats["skipped"] += 1
            continue

        # 定位源文件
        if os.path.exists(db_full_path):
            src_path = db_full_path
        else:
            src_path = os.path.join(db_dir, file_name)
            if not os.path.exists(src_path):
                print("   [WARN] 文件不存在，跳过")
                stats["skipped"] += 1
                continue

        print(f"   源文件: {src_path} ({os.path.getsize(src_path):,} bytes)")

        work_path = os.path.join(qq_bin, file_name)
        clean_path = os.path.join(qq_bin, file_name + ".clean")
        shutil.copy2(src_path, work_path)

        db_t0 = time.perf_counter()
        try:
            result = subprocess.run(
                [exe_in_bin, os.path.basename(work_path), key_hex],
                cwd=qq_bin,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=600,
            )
            db_elapsed = time.perf_counter() - db_t0
            if result.returncode == 0:
                final_path = build_output_path(src_path, db_dir, output_dir)
                os.makedirs(os.path.dirname(final_path), exist_ok=True)

                # pcqq_batch_decrypt.exe 已负责自适应去扩展头，这里直接复制结果
                shutil.copy2(work_path, final_path)

                print(f"   [OK] -> {final_path}  ({fmt_duration(db_elapsed)})")
                stats["success"] += 1
            else:
                print(
                    f"   [ERR] 解密失败 (返回码: {result.returncode})  ({fmt_duration(db_elapsed)})"
                )
                stats["failed"] += 1
        except subprocess.TimeoutExpired:
            print(f"   [ERR] 超时  ({fmt_duration(time.perf_counter() - db_t0)})")
            stats["failed"] += 1
        except Exception as e:
            print(f"   [ERR] 异常: {e}  ({fmt_duration(time.perf_counter() - db_t0)})")
            stats["failed"] += 1
        finally:
            if os.path.exists(clean_path):
                os.remove(clean_path)
            if os.path.exists(work_path) and work_path != src_path:
                os.remove(work_path)

    print(
        f"\n[OK] Phase 2 完成: 成功 {stats['success']}, 失败 {stats['failed']}, 跳过 {stats['skipped']}"
    )
    return stats


# ---------------------------------------------------------------
#  Phase 3: 消息字段解码
# ---------------------------------------------------------------

# ——— 内嵌 Python 解码器（降级方案，来自 decode_preview.py） ———

MSG_TEXT = 1
MSG_FACE = 2
MSG_GROUP_IMAGE = 3
MSG_PRIVATE_IMAGE = 6
MSG_VOICE = 7
MSG_NICKNAME = 18
MSG_VIDEO = 26


class Buffer:
    def __init__(self, data: bytes):
        self.data = data
        self.off = 0

    def empty(self) -> bool:
        return self.off >= len(self.data)

    def read(self, n: int) -> bytes:
        if self.off + n > len(self.data):
            raise ValueError("buffer underflow")
        out = self.data[self.off : self.off + n]
        self.off += n
        return out

    def skip(self, n: int):
        self.read(n)

    def byte(self) -> int:
        return self.read(1)[0]

    def uint16(self) -> int:
        return struct.unpack("<H", self.read(2))[0]

    def uint32(self) -> int:
        return struct.unpack("<I", self.read(4))[0]

    def tlv(self) -> tuple:
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


def decode_text_payload(data: bytes) -> list:
    out = []
    buf = Buffer(data)
    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == 1:
            out.append(decode_utf16le(v))
        elif t == 6:
            at = _decode_at(v)
            if at:
                out.append(at)
    return out


def _decode_at(data: bytes):
    if len(data) < 11:
        return None
    if len(data) >= 7 and data[6] == 1:
        return "@全体成员"
    if len(data) >= 11:
        uin = struct.unpack(">I", data[7:11])[0]
        return f"@{uin}"
    return None


def decode_face_payload(data: bytes):
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


def unpack_message(data: bytes) -> tuple:
    """解析 MsgContent 二进制字段，返回 (decoded_text, sender_nickname)"""
    if len(data) < 26:
        raise ValueError("message too short")

    buf = Buffer(data)
    buf.skip(8)
    buf.uint32()  # time
    buf.uint32()  # rand
    buf.uint32()  # color
    buf.byte()  # font_size
    buf.byte()  # font_style
    buf.byte()  # charset
    buf.byte()  # font_family
    font_name_len = buf.uint16()
    buf.skip(font_name_len)
    buf.skip(2)

    elements = []
    sender_nickname = ""

    while not buf.empty():
        try:
            t, _, v = buf.tlv()
        except ValueError:
            break
        if t == MSG_TEXT:
            elements.extend(decode_text_payload(v))
        elif t == MSG_FACE:
            elem = decode_face_payload(v)
            if elem:
                elements.append(elem)
        elif t == MSG_GROUP_IMAGE:
            elements.append(decode_image_payload(v, "群图片"))
        elif t == MSG_PRIVATE_IMAGE:
            elements.append(decode_image_payload(v, "私聊图片"))
        elif t == MSG_VOICE:
            elements.append(decode_voice_payload(v))
        elif t == MSG_VIDEO:
            elements.append(decode_video_payload(v))
        elif t == MSG_NICKNAME:
            # 解析昵称
            nb = Buffer(v)
            while not nb.empty():
                try:
                    nt, _, nv = nb.tlv()
                except ValueError:
                    break
                if nt in (1, 2):
                    sender_nickname = decode_utf16le(nv)
                    break

    return "".join(elements), sender_nickname


def python_decode_db(db_path: str) -> int:
    """内嵌 Python 解码器: 解码 Msg3.0 数据库中的 MsgContent 字段"""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 找到所有含 MsgContent 的表
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = []
    for (name,) in cur.fetchall():
        if "$" in name:
            continue
        try:
            cur.execute(f"PRAGMA table_info([{name}])")
            cols = {row[1] for row in cur.fetchall()}
        except sqlite3.Error:
            continue
        if "MsgContent" in cols:
            tables.append(name)

    total = 0
    for table_name in tables:
        # 确保 DecodedMsg 列存在
        cur.execute(f"PRAGMA table_info([{table_name}])")
        cols = {row[1] for row in cur.fetchall()}
        if "DecodedMsg" not in cols:
            cur.execute(f"ALTER TABLE [{table_name}] ADD COLUMN DecodedMsg TEXT")
            conn.commit()

        cur.execute(f"SELECT rowid, MsgContent FROM [{table_name}]")
        rows = cur.fetchall()
        counter = 0
        for rowid, msg_content in rows:
            try:
                decoded, _ = unpack_message(msg_content)
            except Exception as e:
                decoded = f"<decode error: {e}>"
            cur.execute(
                f"UPDATE [{table_name}] SET DecodedMsg = ? WHERE rowid = ?",
                (decoded, rowid),
            )
            counter += 1
            if counter % 5000 == 0:
                conn.commit()
                print(f"     [{table_name}] 已处理 {counter} 条")
        conn.commit()
        print(f"   [OK] [{table_name}] 共 {counter} 条")
        total += counter

    conn.close()
    return total


def phase_decode_messages(output_dir: str) -> dict:
    """Phase 3: 解码 Msg3.0.decrypted.db 中的消息字段"""
    print(f"\n{'='*60}")
    print("[Phase 3] 消息字段解码")
    print(f"{'='*60}")

    # 查找 Msg3.0.decrypted.db
    msg_db = None
    for root, _, files in os.walk(output_dir):
        for f in files:
            if f.lower().startswith("msg3.0") and f.lower().endswith(".decrypted.db"):
                msg_db = os.path.join(root, f)
                break
        if msg_db:
            break

    if not msg_db:
        print("[WARN] 未找到 Msg3.0.decrypted.db，跳过解码")
        return {"decoded": False, "reason": "not found"}

    print(f"  目标: {msg_db} ({os.path.getsize(msg_db):,} bytes)")

    # 优先使用 qqdb-decode.exe
    qqdb_exe = SCRIPT_DIR / "qqdb-decode.exe"
    if qqdb_exe.exists():
        print(f"  使用 Rust 版解码器: {qqdb_exe}")
        try:
            decode_t0 = time.perf_counter()
            proc = subprocess.Popen(
                [str(qqdb_exe), msg_db, "--preview-limit", "0"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            last_lines = []
            while True:
                line = proc.stdout.readline()
                if not line and proc.poll() is not None:
                    break
                line = line.rstrip("\n")
                if line.strip():
                    elapsed = time.perf_counter() - decode_t0
                    # 对于 INFO/WARN 输出充坛嵌入已用时
                    print(f"     [{fmt_duration(elapsed)}] {line}")
            proc.wait(timeout=3600)
            print()
            if proc.returncode == 0:
                total_elapsed = time.perf_counter() - decode_t0
                print(f"   [OK] 解码完成 (Rust) -- {fmt_duration(total_elapsed)}")
                return {
                    "decoded": True,
                    "engine": "qqdb-decode.exe",
                    "elapsed": total_elapsed,
                }
            else:
                print(
                    f"   [WARN] qqdb-decode.exe 退出码 {proc.returncode}，尝试 Python 降级"
                )
        except subprocess.TimeoutExpired:
            print("   [WARN] qqdb-decode.exe 超时，尝试 Python 降级")
            proc.kill()
        except Exception as e:
            print(f"   [WARN] qqdb-decode.exe 异常: {e}，尝试 Python 降级")
    else:
        print(f"[WARN] 未找到 qqdb-decode.exe ({qqdb_exe})，使用 Python 解码器")

    # Python 降级解码
    print("  使用内嵌 Python 解码器")
    total = python_decode_db(msg_db)
    print(f"   [OK] 解码完成 (Python)，共处理 {total} 条消息")
    return {"decoded": True, "engine": "python", "total": total}


# ---------------------------------------------------------------
#  主程序
# ---------------------------------------------------------------


def check_qq_not_running():
    """检测当前是否有 QQ 正在运行。如果有，则阻塞提示退出。"""
    try:
        import psutil
    except ImportError:
        return

    while True:
        processes = []
        qq_pids = []
        for pid in psutil.pids():
            try:
                p = psutil.Process(pid)
                if p.name().lower() == "qq.exe":
                    processes.append(p)
                    qq_pids.append(pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        if not processes:
            break

        print(f"\n检测到有 QQ 进程正在运行 (PID: {', '.join(map(str, qq_pids))})")
        input(">>> 按回车键关闭所有 QQ 进程 <<<")

        # 尝试自动关闭
        for p in processes:
            try:
                p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        # 监控它们是否真正退出
        print("正在等待进程退出...")
        _, alive = psutil.wait_procs(processes, timeout=5)

        if alive:
            alive_pids = [p.pid for p in alive]
            print(
                f"[WARN] 无法关闭以下进程: {alive_pids}，请手动利用任务管理器强制结束它们！"
            )
            input(">>> 手动结束后，按回车键重新检查 <<<\n")
        else:
            print("[OK] QQ 进程已全部关闭。")


def main():
    parser = argparse.ArgumentParser(
        description="PCQQ 一键解密流水线",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
示例:
  python pcqq_pipeline.py <QQ数据库目录>                              # 完整流程
  python pcqq_pipeline.py <QQ数据库目录> --skip-keys                  # 已有 keys.json
  python pcqq_pipeline.py <QQ数据库目录> --skip-keys --qq-bin <QQ Bin目录>      # 手动指定 QQ Bin
  python pcqq_pipeline.py --skip-keys --skip-decrypt --decrypt-dir <解密后数据库目录>  # 只做解码
""",
    )
    parser.add_argument("db_dir", nargs="?", default=None, help="QQ 数据库目录（解密阶段必须）")
    parser.add_argument("--keys-file", default="keys.json", help="密钥文件路径")
    parser.add_argument("--decrypt-dir", default=None, help="解密后数据库目录（默认: <db_dir>/_decrypted）")
    parser.add_argument("--qq-bin", default=None, help="QQ 安装目录的 Bin 路径")
    parser.add_argument("--skip-keys", action="store_true", help="跳过密钥获取")
    parser.add_argument("--skip-decrypt", action="store_true", help="跳过解密")
    parser.add_argument("--skip-decode", action="store_true", help="跳过消息解码")

    args = parser.parse_args()

    # 参数校验
    if not args.skip_decrypt and not args.db_dir:
        parser.error("需要解密时必须提供 db_dir（QQ 数据库目录）")

    if args.decrypt_dir is None:
        if args.db_dir:
            args.decrypt_dir = os.path.join(args.db_dir, "_decrypted")
        else:
            parser.error("跳过解密时必须通过 --decrypt-dir 指定解密后数据库目录")

    print("+----------------------------------------------------------+")
    print("|            PCQQ 一键解密流水线                           |")
    print("+----------------------------------------------------------+")
    print(f"  密钥文件: {args.keys_file}")
    if args.db_dir:
        print(f"  数据库目录: {args.db_dir}")
    print(f"  解密目录: {args.decrypt_dir}")
    print(f"  QQ Bin: {args.qq_bin or '自动查找'}")
    print(
        f"  跳过: {'密钥' if args.skip_keys else '' + ('解密' if args.skip_decrypt else '') + ('解码' if args.skip_decode else '') or "无"}"
    )

    pipeline_start = time.perf_counter()
    timings = {}

    # Phase 1
    if not args.skip_keys:
        # 获取密钥前需要先关闭 QQ，再由用户手动打开
        check_qq_not_running()

        print("\n" + "─" * 60)
        input(">>> 打开 QQ 出现登录界面后，按回车键注入 Hook <<<\n")

        t0 = time.perf_counter()
        phase_get_keys(args.keys_file)
        timings["Phase 1 密钥获取"] = time.perf_counter() - t0
    else:
        print(f"\n-- 跳过 Phase 1 (密钥获取)")

    # Phase 2
    if not args.skip_decrypt:
        t0 = time.perf_counter()
        decrypt_stats = phase_decrypt_all(
            args.keys_file, args.db_dir, args.decrypt_dir, args.qq_bin
        )
        timings["Phase 2 批量解密"] = time.perf_counter() - t0
    else:
        print(f"\n-- 跳过 Phase 2 (解密)")
        decrypt_stats = None

    # Phase 3
    if not args.skip_decode:
        t0 = time.perf_counter()
        decode_result = phase_decode_messages(args.decrypt_dir)
        timings["Phase 3 消息解码"] = time.perf_counter() - t0
    else:
        print(f"\n-- 跳过 Phase 3 (解码)")
        decode_result = None

    total_time = time.perf_counter() - pipeline_start

    # 汇总
    print(f"\n{'='*60}")
    print("流水线执行完毕")
    if decrypt_stats:
        print(
            f"  解密: 成功 {decrypt_stats['success']}, 失败 {decrypt_stats['failed']}, 跳过 {decrypt_stats['skipped']}"
        )
    if decode_result and decode_result.get("decoded"):
        engine = decode_result.get("engine", "?")
        print(f"  解码: Msg3.0.decrypted.db — 引擎={engine}")
    print(f"  输出: {args.decrypt_dir}")
    print(f"\n耗时统计:")
    for name, dur in timings.items():
        print(f"  {name}: {fmt_duration(dur)}")
    print(f"  总耗时: {fmt_duration(total_time)}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
