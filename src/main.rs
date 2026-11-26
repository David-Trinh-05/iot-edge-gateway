mod mqtt;
mod protocol;
mod store;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use mqtt::Mqtt;
use protocol::{Telemetry, valid_name};
use serde_json::json;
use std::{
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use store::{Admission, Store};
use tokio::{net::TcpListener, sync::Semaphore};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy)]
enum Role {
    Gateway,
    Receiver,
}

impl Role {
    fn name(self) -> &'static str {
        match self {
            Self::Gateway => "gateway",
            Self::Receiver => "receiver",
        }
    }
}

#[derive(Clone)]
struct Api {
    db: Store,
    mqtt_ready: Arc<AtomicBool>,
    pending_limit: i64,
    db_slots: Arc<Semaphore>,
}

impl Api {
    async fn ingest(State(api): State<Self>, Json(event): Json<Telemetry>) -> Response {
        if let Err(message) = event.validate() {
            return response(StatusCode::UNPROCESSABLE_ENTITY, message);
        }
        let Ok(_permit) = api.db_slots.try_acquire() else {
            return backpressure("database busy");
        };
        let payload = match serde_json::to_string(&event) {
            Ok(payload) => payload,
            Err(error) => return storage_error(error),
        };

        match api
            .db
            .admit(event.id.clone(), payload, api.pending_limit)
            .await
        {
            Ok(Admission::Accepted) => {
                (StatusCode::ACCEPTED, Json(json!({ "id": event.id }))).into_response()
            }
            Ok(Admission::Conflict) => response(
                StatusCode::CONFLICT,
                "event ID already has different content",
            ),
            Ok(Admission::Full) => backpressure("offline queue at capacity"),
            Err(error) => storage_error(error),
        }
    }

    async fn status(State(api): State<Self>) -> Response {
        let Ok(_permit) = api.db_slots.try_acquire() else {
            return backpressure("database busy");
        };
        match api.db.counts().await {
            Ok(counts) => Json(json!({
                "accepted": counts.accepted,
                "delivered": counts.delivered,
                "pending": counts.pending,
                "pending_limit": api.pending_limit,
                "mqtt_subscribed": api.mqtt_ready.load(Ordering::Relaxed),
            }))
            .into_response(),
            Err(error) => storage_error(error),
        }
    }
}

fn response(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn backpressure(message: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(RETRY_AFTER, "1")],
        Json(json!({ "error": message })),
    )
        .into_response()
}

fn storage_error(error: impl std::fmt::Display) -> Response {
    eprintln!("gateway storage failed: {error}");
    backpressure("storage unavailable")
}

fn optional_setting(key: &str) -> Result<Option<String>> {
    match env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!("invalid {key}: {error}").into()),
    }
}

fn setting(key: &str, default: &str) -> Result<String> {
    Ok(optional_setting(key)?.unwrap_or_else(|| default.to_owned()))
}

async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    let role = match args.as_slice() {
        [command] if command == "gateway" => Role::Gateway,
        [command] if command == "receiver" => Role::Receiver,
        _ => {
            eprintln!("usage: iot-edge-gateway gateway|receiver (see README for settings)");
            std::process::exit(2);
        }
    };

    let id = setting("IOT_GATEWAY_ID", "lab")?;
    if !valid_name(&id, 24) {
        return Err("IOT_GATEWAY_ID must be a valid identifier of up to 24 characters".into());
    }
    let path = env::var_os("IOT_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("data/{}.sqlite3", role.name())));
    let db = Store::open(&path, role.name(), &id)?;
    let mqtt = Mqtt::from_env(role, id)?;
    let ready = Arc::new(AtomicBool::new(false));

    match role {
        Role::Receiver => {
            tokio::select! {
                _ = mqtt.run(db, ready) => {}
                _ = shutdown() => {}
            }
        }
        Role::Gateway => {
            let address: SocketAddr = setting("IOT_HTTP_ADDR", "127.0.0.1:8080")?.parse()?;
            let pending_limit: i64 = setting("IOT_MAX_PENDING", "10000")?.parse()?;
            if pending_limit <= 0 {
                return Err("IOT_MAX_PENDING must be positive".into());
            }
            let listener = TcpListener::bind(address).await?;
            let router = Router::new()
                .route("/v1/telemetry", post(Api::ingest))
                .route("/status", get(Api::status))
                .layer(DefaultBodyLimit::max(4096))
                .with_state(Api {
                    db: db.clone(),
                    mqtt_ready: Arc::clone(&ready),
                    pending_limit,
                    db_slots: Arc::new(Semaphore::new(16)),
                });
            eprintln!("gateway listening on {}", listener.local_addr()?);
            let worker = tokio::spawn(mqtt.run(db, ready));
            let result = axum::serve(listener, router)
                .with_graceful_shutdown(shutdown())
                .await;
            worker.abort();
            let _ = worker.await;
            result?;
        }
    }
    Ok(())
}
