use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Telemetry {
    pub id: String,
    pub device_id: String,
    pub timestamp_ms: i64,
    pub metric: String,
    pub value: f64,
}

impl Telemetry {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_name(&self.id, 96) {
            return Err("invalid event ID");
        }
        if !valid_name(&self.device_id, 64) {
            return Err("invalid device ID");
        }
        if !valid_name(&self.metric, 64) {
            return Err("invalid metric name");
        }
        if self.timestamp_ms < 0 {
            return Err("timestamp_ms must be nonnegative");
        }
        if !self.value.is_finite() {
            return Err("value must be finite");
        }
        Ok(())
    }
}

pub fn valid_name(value: &str, max_len: usize) -> bool {
    value.len() <= max_len
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

pub fn fingerprint(payload: &str) -> String {
    format!("{:x}", Sha256::digest(payload.as_bytes()))
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub id: String,
    pub fingerprint: String,
}
