use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum DecodedValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Map(HashMap<String, String>),
}

pub fn decode_command_payload(code: u8, payload: &[u8]) -> Option<HashMap<String, DecodedValue>> {
    if payload.is_empty() {
        return None;
    }

    let data = &payload[1..];
    let mut map = HashMap::new();

    match code {
        0x01 if payload.len() >= 3 => {
            map.insert("version".into(), DecodedValue::Integer(payload[1] as i64));
            let app = String::from_utf8_lossy(&payload[2..])
                .trim_end_matches('\0')
                .to_string();
            map.insert("app".into(), DecodedValue::String(app));
        }
        0x02 if payload.len() >= 7 => {
            let msg_type = payload[1];
            let attempt = payload[2];
            let timestamp = u32::from_le_bytes(payload[3..7].try_into().unwrap());
            let dst = if payload.len() > 7 {
                hex::encode(&payload[7..13.min(payload.len())])
            } else {
                String::new()
            };
            let text = if payload.len() > 13 {
                String::from_utf8_lossy(&payload[13..]).to_string()
            } else {
                String::new()
            };
            map.insert("type".into(), DecodedValue::Integer(msg_type as i64));
            map.insert("attempt".into(), DecodedValue::Integer(attempt as i64));
            map.insert("timestamp".into(), DecodedValue::Integer(timestamp as i64));
            map.insert("to".into(), DecodedValue::String(dst));
            map.insert("text".into(), DecodedValue::String(text));
        }
        0x03 if payload.len() >= 7 => {
            let channel = payload[2];
            let timestamp = u32::from_le_bytes(payload[3..7].try_into().unwrap());
            let text = if payload.len() > 7 {
                String::from_utf8_lossy(&payload[7..]).to_string()
            } else {
                String::new()
            };
            map.insert("channel".into(), DecodedValue::Integer(channel as i64));
            map.insert("timestamp".into(), DecodedValue::Integer(timestamp as i64));
            map.insert("text".into(), DecodedValue::String(text));
        }
        0x04 if payload.len() > 1 => {
            let lastmod = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("lastmod".into(), DecodedValue::Integer(lastmod as i64));
        }
        0x06 if payload.len() >= 5 => {
            let time = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("time".into(), DecodedValue::Integer(time as i64));
        }
        0x08 => {
            let name = String::from_utf8_lossy(data)
                .trim_end_matches('\0')
                .to_string();
            map.insert("name".into(), DecodedValue::String(name));
        }
        0x0B if payload.len() >= 11 => {
            let freq = f64::from(u32::from_le_bytes(payload[1..5].try_into().unwrap())) / 1000.0;
            let bw = f64::from(u32::from_le_bytes(payload[5..9].try_into().unwrap())) / 1000.0;
            let sf = payload[9];
            let cr = payload[10];
            map.insert("freq_mhz".into(), DecodedValue::Float(freq));
            map.insert("bw_khz".into(), DecodedValue::Float(bw));
            map.insert("sf".into(), DecodedValue::Integer(sf as i64));
            map.insert("cr".into(), DecodedValue::Integer(cr as i64));
        }
        0x0C if payload.len() >= 5 => {
            let power = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("tx_power".into(), DecodedValue::Integer(power as i64));
        }
        0x0E if payload.len() >= 9 => {
            let lat =
                f64::from(i32::from_le_bytes(payload[1..5].try_into().unwrap())) / 1_000_000.0;
            let lon =
                f64::from(i32::from_le_bytes(payload[5..9].try_into().unwrap())) / 1_000_000.0;
            map.insert("lat".into(), DecodedValue::Float(lat));
            map.insert("lon".into(), DecodedValue::Float(lon));
        }
        0x14 => {}
        0x16 => {
            map.insert("query".into(), DecodedValue::String("device_info".into()));
        }
        0x1A if payload.len() > 33 => {
            let dst = hex::encode(&payload[1..33]);
            let _pwd = String::from_utf8_lossy(&payload[33..]).to_string();
            map.insert("to".into(), DecodedValue::String(dst));
            map.insert("password".into(), DecodedValue::String("***".into()));
        }
        0x1F if payload.len() > 1 => {
            map.insert(
                "channel_idx".into(),
                DecodedValue::Integer(payload[1] as i64),
            );
        }
        0x20 if payload.len() > 49 => {
            let idx = payload[1];
            let name = String::from_utf8_lossy(&payload[2..34])
                .trim_end_matches('\0')
                .to_string();
            let psk = hex::encode(&payload[34..50]);
            map.insert("channel_idx".into(), DecodedValue::Integer(idx as i64));
            map.insert("name".into(), DecodedValue::String(name));
            map.insert("psk".into(), DecodedValue::String(psk));
        }
        0x25 if payload.len() >= 5 => {
            let pin = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("pin".into(), DecodedValue::Integer(pin as i64));
        }
        0x27 => {
            if payload.len() > 4 {
                let target = hex::encode(&payload[4..10.min(payload.len())]);
                map.insert("target".into(), DecodedValue::String(target));
            } else {
                map.insert("target".into(), DecodedValue::String("self".into()));
            }
        }
        0x34 if payload.len() > 2 => {
            let target = hex::encode(&payload[2..34.min(payload.len())]);
            map.insert("target".into(), DecodedValue::String(target));
        }
        0x38 if payload.len() > 1 => {
            let st = payload[1];
            let name = match st {
                0 => "core",
                1 => "radio",
                2 => "packets",
                _ => "unknown",
            };
            map.insert("stats_type".into(), DecodedValue::String(name.into()));
        }
        _ => {}
    }

    if map.is_empty() { None } else { Some(map) }
}

pub fn decode_response_payload(code: u8, payload: &[u8]) -> Option<HashMap<String, DecodedValue>> {
    if payload.is_empty() {
        return None;
    }

    let _data = &payload[1..];
    let mut map = HashMap::new();

    match code {
        0x00 => {
            map.insert("status".into(), DecodedValue::String("OK".into()));
            if payload.len() == 5 {
                let val = u32::from_le_bytes(payload[1..5].try_into().unwrap());
                map.insert("value".into(), DecodedValue::Integer(val as i64));
            }
        }
        0x01 => {
            map.insert("status".into(), DecodedValue::String("ERROR".into()));
            if payload.len() > 1 {
                map.insert(
                    "error_code".into(),
                    DecodedValue::Integer(payload[1] as i64),
                );
            }
        }
        0x02 if payload.len() >= 5 => {
            let count = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("contact_count".into(), DecodedValue::Integer(count as i64));
        }
        0x03 if payload.len() >= 148 => {
            let pk = hex::encode(&payload[1..33]);
            let contact_type = payload[33];
            let _flags = payload[34];
            let path_len = payload[35] as i64;
            let name = String::from_utf8_lossy(
                &payload[100..][..payload[100..].iter().position(|&b| b == 0).unwrap_or(32)],
            )
            .to_string();
            let last_advert = u32::from_le_bytes(payload[132..136].try_into().unwrap());
            let lat =
                f64::from(i32::from_le_bytes(payload[136..140].try_into().unwrap())) / 1_000_000.0;
            let lon =
                f64::from(i32::from_le_bytes(payload[140..144].try_into().unwrap())) / 1_000_000.0;
            let type_name = match contact_type {
                0 => "node",
                1 => "repeater",
                2 => "room",
                _ => "unknown",
            };
            map.insert("name".into(), DecodedValue::String(name));
            map.insert(
                "public_key".into(),
                DecodedValue::String(format!("{}...", &pk[..12])),
            );
            map.insert("type".into(), DecodedValue::String(type_name.into()));
            map.insert("path_len".into(), DecodedValue::Integer(path_len));
            map.insert(
                "last_advert".into(),
                DecodedValue::Integer(last_advert as i64),
            );
            if lat != 0.0 {
                map.insert("lat".into(), DecodedValue::Float(lat));
            }
            if lon != 0.0 {
                map.insert("lon".into(), DecodedValue::Float(lon));
            }
        }
        0x04 if payload.len() >= 5 => {
            let lastmod = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("lastmod".into(), DecodedValue::Integer(lastmod as i64));
        }
        0x05 if payload.len() >= 52 => {
            let adv_type = payload[1];
            let tx_power = payload[2];
            let pk = hex::encode(&payload[4..36]);
            let lat =
                f64::from(i32::from_le_bytes(payload[36..40].try_into().unwrap())) / 1_000_000.0;
            let lon =
                f64::from(i32::from_le_bytes(payload[40..44].try_into().unwrap())) / 1_000_000.0;
            let freq = f64::from(u32::from_le_bytes(payload[48..52].try_into().unwrap())) / 1000.0;
            let bw = if payload.len() >= 56 {
                f64::from(u32::from_le_bytes(payload[52..56].try_into().unwrap())) / 1000.0
            } else {
                0.0
            };
            let type_name = match adv_type {
                0 => "node",
                1 => "client",
                2 => "repeater",
                3 => "room",
                _ => "unknown",
            };
            let name = if payload.len() > 56 {
                String::from_utf8_lossy(&payload[56..])
                    .trim_end_matches('\0')
                    .to_string()
            } else {
                String::new()
            };
            map.insert("name".into(), DecodedValue::String(name));
            map.insert("type".into(), DecodedValue::String(type_name.into()));
            map.insert(
                "public_key".into(),
                DecodedValue::String(format!("{}...", &pk[..12])),
            );
            map.insert("tx_power".into(), DecodedValue::Integer(tx_power as i64));
            map.insert("freq_mhz".into(), DecodedValue::Float(freq));
            map.insert("bw_khz".into(), DecodedValue::Float(bw));
            if lat != 0.0 {
                map.insert("lat".into(), DecodedValue::Float(lat));
            }
            if lon != 0.0 {
                map.insert("lon".into(), DecodedValue::Float(lon));
            }
        }
        0x06 if payload.len() >= 9 => {
            let msg_type = payload[1];
            let ack = hex::encode(&payload[2..6]);
            let timeout = u32::from_le_bytes(payload[6..10].try_into().unwrap());
            map.insert("msg_type".into(), DecodedValue::Integer(msg_type as i64));
            map.insert("expected_ack".into(), DecodedValue::String(ack));
            map.insert("timeout_ms".into(), DecodedValue::Integer(timeout as i64));
        }
        0x07 if payload.len() >= 13 => {
            let from = hex::encode(&payload[1..7]);
            let path_len = payload[7];
            let txt_type = payload[8];
            let timestamp = u32::from_le_bytes(payload[9..13].try_into().unwrap());
            let text = String::from_utf8_lossy(&payload[13..]).to_string();
            map.insert("from".into(), DecodedValue::String(from));
            map.insert("path_len".into(), DecodedValue::Integer(path_len as i64));
            map.insert("timestamp".into(), DecodedValue::Integer(timestamp as i64));
            map.insert("text".into(), DecodedValue::String(text));
            let type_name = match txt_type {
                0 => "text",
                1 => "command",
                2 => "signed",
                _ => "unknown",
            };
            map.insert("type".into(), DecodedValue::String(type_name.into()));
        }
        0x08 if payload.len() >= 9 => {
            let channel = payload[1];
            let path_len = payload[2];
            let txt_type = payload[3];
            let timestamp = u32::from_le_bytes(payload[4..8].try_into().unwrap());
            let text = String::from_utf8_lossy(&payload[8..]).to_string();
            map.insert("channel".into(), DecodedValue::Integer(channel as i64));
            map.insert("path_len".into(), DecodedValue::Integer(path_len as i64));
            map.insert("timestamp".into(), DecodedValue::Integer(timestamp as i64));
            map.insert("text".into(), DecodedValue::String(text));
            let type_name = match txt_type {
                0 => "text",
                1 => "command",
                _ => "unknown",
            };
            map.insert("type".into(), DecodedValue::String(type_name.into()));
        }
        0x09 if payload.len() >= 5 => {
            let time = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            map.insert("time".into(), DecodedValue::Integer(time as i64));
        }
        0x0A => {
            map.insert("messages_available".into(), DecodedValue::Bool(false));
        }
        0x0C if payload.len() >= 3 => {
            let mv = u16::from_le_bytes(payload[1..3].try_into().unwrap());
            map.insert("level_mv".into(), DecodedValue::Integer(mv as i64));
            if payload.len() >= 11 {
                let used = u32::from_le_bytes(payload[3..7].try_into().unwrap());
                let total = u32::from_le_bytes(payload[7..11].try_into().unwrap());
                map.insert("used_kb".into(), DecodedValue::Integer(used as i64));
                map.insert("total_kb".into(), DecodedValue::Integer(total as i64));
            }
        }
        0x0D if payload.len() > 1 => {
            let fw = payload[1];
            map.insert("fw_version".into(), DecodedValue::Integer(fw as i64));
            if payload.len() > 60 {
                map.insert(
                    "max_contacts".into(),
                    DecodedValue::Integer((payload[2] as u16 * 2) as i64),
                );
                map.insert(
                    "max_channels".into(),
                    DecodedValue::Integer(payload[3] as i64),
                );
                let build = String::from_utf8_lossy(&payload[8..20])
                    .trim_end_matches('\0')
                    .to_string();
                let model = String::from_utf8_lossy(&payload[20..60])
                    .trim_end_matches('\0')
                    .to_string();
                map.insert("fw_build".into(), DecodedValue::String(build));
                map.insert("model".into(), DecodedValue::String(model));
            }
        }
        0x12 if payload.len() > 2 => {
            let idx = payload[1];
            let name_end = payload[2..34].iter().position(|&b| b == 0).unwrap_or(32);
            let name = String::from_utf8_lossy(&payload[2..2 + name_end]).to_string();
            map.insert("channel_idx".into(), DecodedValue::Integer(idx as i64));
            map.insert("name".into(), DecodedValue::String(name));
        }
        0x82 => {
            if payload.len() >= 5 {
                let ack = hex::encode(&payload[1..5]);
                map.insert("ack_code".into(), DecodedValue::String(ack));
            } else {
                map.insert("ack".into(), DecodedValue::Bool(true));
            }
        }
        0x83 => {
            map.insert("messages_waiting".into(), DecodedValue::Bool(true));
        }
        0x85 => {
            map.insert("login".into(), DecodedValue::String("success".into()));
        }
        0x86 => {
            map.insert("login".into(), DecodedValue::String("failed".into()));
        }
        _ => {}
    }

    if map.is_empty() { None } else { Some(map) }
}

fn format_entry(k: &str, v: &DecodedValue) -> String {
    let val_str = match v {
        DecodedValue::String(s) => s.clone(),
        DecodedValue::Integer(i) => i.to_string(),
        DecodedValue::Float(f) => format!("{f:.2}"),
        DecodedValue::Bool(b) => b.to_string(),
        DecodedValue::Bytes(b) => hex::encode(b),
        DecodedValue::Map(m) => {
            let inner: Vec<String> = m.iter().map(|(ik, iv)| format!("{ik}={iv}")).collect();
            format!("{{{}}}", inner.join(", "))
        }
    };
    format!("{k}={val_str}")
}

fn should_skip(v: &DecodedValue) -> bool {
    match v {
        DecodedValue::Float(f) => *f == 0.0,
        _ => false,
    }
}

pub fn format_decoded(map: &HashMap<String, DecodedValue>) -> String {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();

    let mut parts: Vec<String> = Vec::with_capacity(keys.len());

    // name first if present
    if let Some(v) = map.get("name")
        && !should_skip(v)
    {
        parts.push(format_entry("name", v));
    }

    for k in keys {
        if *k == "name" {
            continue;
        }
        let v = &map[k];
        if should_skip(v) {
            continue;
        }
        parts.push(format_entry(k, v));
    }

    parts.join(" | ")
}
