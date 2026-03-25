# qqdb-decode

> **Readme和大部分代码由codex由gpt5.4 xhigh生成，未经仔细审查！**

解码解密后的 QQ 数据库的 `MsgContent`聊天记录，并提供抓密钥、数据库解密和排查工具。

## 构建

### Rust

```powershell
cargo build --release --bin qqdb-decode
cargo build --release --bin analyze
cargo build --release --bin decode_row
cargo build --release --bin runtime_prep
cargo build --release --bin profile_cold
```

### C++

需要 32 位 MinGW-W64：

```powershell
g++ pcqq_batch_decrypt.cpp -o pcqq_batch_decrypt.exe -Wall
```

## 快速使用

### 1. 一键流程

`pcqq_pipeline.py` 会依次：

1. 抓数据库密钥，保存到 `keys.json`
2. 解密数据库，输出 `*.decrypted.db`
3. 解码 `Msg3.0.decrypted.db`

先安装依赖：

```powershell
pip install frida frida-tools psutil
```

完整流程：

```powershell
python pcqq_pipeline.py <QQ数据库目录>
```

跳过抓密钥：

```powershell
python pcqq_pipeline.py <QQ数据库目录> --skip-keys
```

手动指定 QQ Bin：

```powershell
python pcqq_pipeline.py <QQ数据库目录> --skip-keys --qq-bin <QQ Bin目录>
```

只做解码：

```powershell
python pcqq_pipeline.py --skip-keys --skip-decrypt --decrypt-dir <解密后数据库目录>
```

默认行为：

- `--keys-file=keys.json`
- `--decrypt-dir=<db_dir>/_decrypted`
- `--qq-bin` 不传时自动查找
- 不加 `--skip-keys`、`--skip-decrypt`、`--skip-decode` 时，三步都会执行

### 2. 直接解码数据库

```text
qqdb-decode <db_path> [--index-db PATH] [--refs-dir DIR] [--preview-limit N] [--no-update-db] [--with-detail-json] [--conservative] [--batch-size N] [--update-limit N] [--table-like SUBSTR]
```

查看帮助：

```powershell
cargo run --release --bin qqdb-decode -- --help
```

只预览：

```powershell
cargo run --release --bin qqdb-decode -- <Msg3.0.decrypted.db> --no-update-db --preview-limit 3
```

写回结果：

```powershell
cargo run --release --bin qqdb-decode -- <Msg3.0.decrypted.db> --preview-limit 0
```

带索引库和详细调试信息：

```powershell
cargo run --release --bin qqdb-decode -- <Msg3.0.decrypted.db> --index-db <Msg3.0index.decrypted.db> --refs-dir <参考数据库目录> --with-detail-json
```

所有参数：

- `<db_path>`：要处理的数据库路径
- `--help` / `-h`：显示帮助
- `--index-db`：默认自动尝试同目录的 `Msg3.0index.decrypted.db`
- `--refs-dir`：默认数据库所在目录
- `--preview-limit=3`：预览输出每张表前 N 条解码结果
- `--no-update-db`：不写回解码结果到数据库
- `--with-detail-json`：写入json格式解码结果到 `DecodedDetailJson` 列
- `--conservative`：使用更安全但更慢的保守写回策略（开启 SQLite 日志和同步，防止意外中断导致数据库损坏）
- `--batch-size=200000`：每批处理的行数
- `--update-limit`：写回的行数上限，默认不限
- `--table-like`：只处理表名包含指定子串的表，默认处理所有含 `MsgContent` 的表

写回字段：

- `DecodedMsg`
- `SenderDisplay`
- `DecodedDetailJson`

### 3. 只解密数据库

`pcqq_batch_decrypt.exe` 必须放在 QQ 安装目录的 `Bin` 文件夹里运行。

解密单个数据库：

```powershell
pcqq_batch_decrypt.exe Msg3.0.db aabbccdd11223344aabbccdd11223344
```

默认行为：

- 直接解密会原地修改数据库
- 直接解密后会自动判断并去除 QQ 扩展头

## 其他工具

分类看样本：

```powershell
cargo run --release --bin analyze -- <Msg3.0.decrypted.db> --index-db <Msg3.0index.decrypted.db> --sample-limit 5
```

默认行为：

- `--sample-limit=3`
- `--index-db` 不传时自动尝试同目录索引库

查看单条消息：

```powershell
cargo run --release --bin decode_row -- <Msg3.0.decrypted.db> <表名> <rowid> --index-db <Msg3.0index.decrypted.db> --refs-dir <参考数据库目录> --json
```

默认行为：

- 不加 `--json` 时输出普通文本
- `--index-db`、`--refs-dir` 默认不指定

导出动态分析样本：

```powershell
cargo run --release --bin runtime_prep -- <Msg3.0.decrypted.db> --index-db <Msg3.0index.decrypted.db> --output-dir runtime_refs
```

默认行为：

- `--output-dir=runtime_artifacts`
- `--sample-limit=5`

测试写回性能：

```powershell
cargo run --release --bin profile_cold -- <Msg3.0.decrypted.db> --index-db <Msg3.0index.decrypted.db> --refs-dir <参考数据库目录> --batch-size 100000
```

默认行为：

- `--batch-size=100000`
- `--update-limit` 不限
- `--table-like` 处理所有表
- `--conservative` 关闭

Python 版预览/写回：

```powershell
python decode_preview.py <Msg3.0.decrypted.db> --limit-per-table 20 --output decoded_preview.csv
python decode_preview.py <Msg3.0.decrypted.db> --update-db --limit-per-table 1000
```

默认行为：

- `--limit-per-table=20`
- `--output=decoded_preview.csv`
- 不加 `--update-db` 时只导出预览
