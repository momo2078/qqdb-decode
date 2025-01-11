use std::path::PathBuf;

use anyhow::Result;
use colored::Colorize;
use rusqlite::Connection;
use tracing::{event, Level};

mod elements;
mod raw;

/*
https://github.com/Icalingua-plus-plus/oicq-icalingua-plus-plus/blob/3b6d2047f0c898efaf53a0075dd5d24dca234877/lib/common.js#L108-L127
function uin2code(groupUin) {
    let left = Math.floor(groupUin / 1000000);
    if (left >= 202 && left <= 212)
        left -= 202;
    else if (left >= 480 && left <= 488)
        left -= 469;
    else if (left >= 2100 && left <= 2146)
        left -= 2080;
    else if (left >= 2010 && left <= 2099)
        left -= 1943;
    else if (left >= 2147 && left <= 2199)
        left -= 1990;
    else if (left >= 2600 && left <= 2651)
        left -= 2265;
    else if (left >= 3800 && left <= 3989)
        left -= 3490;
    else if (left >= 4100 && left <= 4199)
        left -= 3890;
    return left * 1000000 + groupUin % 1000000;
}
     */
pub fn uin_2_code(uin: u64) -> u64 {
    let left = uin / 1_000_000;
    uin % 1_000_000
        + (match left {
            202..=212 => left - 202,
            480..=488 => left - 469,
            2100..=2146 => left - 2080,
            2010..=2099 => left - 1943,
            2147..=2199 => left - 1990,
            2600..=2651 => left - 2265,
            3800..=3989 => left - 3490,
            4100..=4199 => left - 3890,
            x => x,
        } * 1_000_000)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();
    // 获取第一个命令行参数
    let db_path = PathBuf::from({
        let mut args = std::env::args();
        args.next().unwrap();
        args.next().unwrap_or_else(|| {
            panic!(
                "{}",
                "db path not provided, usage: db path".red().to_string()
            )
        })
    });
    // 打印日志
    event!(Level::INFO, "正在转换 {:?} 数据库", db_path);
    // 校验一下路径
    if !db_path.exists() {
        event!(Level::ERROR, "数据库文件 {:?} 不存在", db_path);
        return Ok(());
    }
    if !db_path.is_file() {
        event!(Level::ERROR, "{:?} 不是一个文件", db_path);
        return Ok(());
    }
    // 打开数据库
    let conn = match open_db(&db_path) {
        Ok(conn) => conn,
        Err(e) => {
            event!(Level::ERROR, "打开数据库失败: {:?}", e);
            return Ok(());
        }
    };

    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;

    let friends: Vec<u64> = {
        // 打开 friend.csv
        let raw = std::fs::read_to_string("friend.csv")?;
        // 逐行读取
        let mut friends = Vec::new();
        for line in raw.lines() {
            // 先分出第1~2个逗号之间的部分
            let line = line.split(',').nth(1).unwrap();
            // 转换成数字
            let uin = line.parse::<u64>().unwrap();
            friends.push(uin);
        }
        friends
    };

    let groups: Vec<u64> = {
        // 打开 group.csv
        let raw = std::fs::read_to_string("group.csv")?;
        // 逐行读取
        let mut groups = Vec::new();
        for line in raw.lines() {
            // 先分出第1个逗号之前的部分
            let line = line.split(',').next().unwrap();
            // 转换成数字
            let uin = line.parse::<u64>().unwrap();
            groups.push(uin);
        }
        groups
    };

    // 输出数据库中的所有的表
    // 获取所有的表名
    let tables = stmt.query_map([], |row| row.get::<_, String>(0))?;

    for table_name in tables {
        if table_name.is_err() {
            continue;
        }
        let table_name = table_name.unwrap();
        if !table_name.contains("$")
            && (table_name.contains("group") || table_name.contains("buddy"))
        {
            if table_name.contains("buddy") {
                // 检验是不是在好友列表里
                let uin = table_name
                    .split('_')
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                if !friends.contains(&uin) {
                    // 警告
                    // event!(Level::WARN, "好友表 {} 不在好友列表里", table_name);
                }
            }
            if table_name.contains("group") {
                // 检验是不是在群列表里
                let uin = table_name
                    .split('_')
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                if !groups.contains(&uin) {
                    // 警告
                    // 感谢 oicq
                    // https://github.com/Icalingua-plus-plus/oicq-icalingua-plus-plus/blob/3b6d2047f0c898efaf53a0075dd5d24dca234877/lib/common.js#L108-L127
                    let possible_group_uin = uin_2_code(uin);
                    event!(
                        Level::WARN,
                        "群表 {} 不在群列表里, 找到可能匹配的: {:?}, 结果 {}",
                        table_name,
                        possible_group_uin,
                        if groups.contains(&possible_group_uin) {
                            "✅存在"
                        } else {
                            "❌不存在"
                        }
                    );
                    if !groups.contains(&possible_group_uin) {
                        let mut stmt =
                            conn.prepare(&format!("SELECT * FROM {} limit 50", table_name))?;
                        // let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table_name))?;
                        let rows = stmt.query_map([], |row| {
                            let time: i64 = row.get(0)?;
                            let rand: i64 = row.get(1)?;
                            let sender_uin: i64 = row.get(2)?;
                            let msg_content: Vec<u8> = row.get(3)?;
                            let info: Vec<u8> = row.get(4)?;
                            Ok(raw::RawData::new(time, rand, sender_uin, msg_content, info))
                        })?;
                        for row in rows {
                            if row.is_err() {
                                continue;
                            }
                            let row = row.unwrap();
                            let data = row.decode();
                            // event!(Level::INFO, "找到数据: {}", data);
                        }
                    }
                } else {
                    event!(Level::DEBUG, "找到群: {}", table_name);
                }
            } else {
                // event!(Level::DEBUG, "找到表: {}", table_name);
            }
            // 随便选一个出来
        }
    }

    Ok(())
}

fn open_db(db_path: &PathBuf) -> Result<Connection> {
    Ok(Connection::open(db_path)?)
}
