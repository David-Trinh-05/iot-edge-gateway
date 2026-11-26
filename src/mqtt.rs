use crate::{
    Result, Role,
    protocol::{Receipt, Telemetry, fingerprint},
    store::Store,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, SubscribeReasonCode, Transport};
use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, time};

const WINDOW: usize = 32;
const RETRY_RECEIPT: Duration = Duration::from_secs(2);
const QOS: QoS = QoS::AtLeastOnce;

pub struct Mqtt {
    pub role: Role,
    pub gateway_id: String,
    pub options: MqttOptions,
}

impl Mqtt {
    pub fn from_env(role: Role, gateway_id: String) -> Result<Self> {
        let host = crate::setting("IOT_MQTT_HOST", "127.0.0.1")?;
        let port = crate::setting("IOT_MQTT_PORT", "1883")?.parse::<u16>()?;
        if host.trim().is_empty() || port == 0 {
            return Err("MQTT requires a nonempty host and nonzero port".into());
        }
        let name = format!("{}-{gateway_id}", role.name());
        let mut options = MqttOptions::new(name, host, port);
        options
            .set_clean_session(true)
            .set_keep_alive(Duration::from_secs(5))
            .set_max_packet_size(8192, 8192)
            .set_inflight(64);

        if let Some(path) = std::env::var_os("IOT_MQTT_CA_FILE") {
            options.set_transport(Transport::tls(std::fs::read(path)?, None, None));
        }
        match (
            crate::optional_setting("IOT_MQTT_USERNAME")?,
            crate::optional_setting("IOT_MQTT_PASSWORD")?,
        ) {
            (Some(user), Some(password)) => {
                options.set_credentials(user, password);
            }
            (None, None) => {}
            _ => return Err("set both IOT_MQTT_USERNAME and IOT_MQTT_PASSWORD".into()),
        }

        Ok(Self {
            role,
            gateway_id,
            options,
        })
    }

    pub async fn run(self, db: Store, ready: Arc<AtomicBool>) {
        let minimum = Duration::from_secs(1);
        let mut delay = minimum;
        loop {
            let outcome = self.session(&db, &ready).await;
            if ready.swap(false, Ordering::Relaxed) {
                delay = minimum;
            }
            if let Err(error) = outcome {
                eprintln!(
                    "{} MQTT session ended: {error}; retrying in {}s",
                    self.role.name(),
                    delay.as_secs()
                );
            }
            time::sleep(delay).await;
            delay = (2 * delay).min(Duration::from_secs(10));
        }
    }

    async fn session(&self, db: &Store, ready: &AtomicBool) -> Result<()> {
        let prefix = format!("edge/v1/{}", self.gateway_id);
        let telemetry_topic = format!("{prefix}/telemetry");
        let receipt_topic = format!("{prefix}/receipt");
        let topic = match self.role {
            Role::Gateway => &receipt_topic,
            Role::Receiver => &telemetry_topic,
        };

        let (client, mut event_loop) = AsyncClient::new(self.options.clone(), 128);
        let (send, mut events) = mpsc::channel(64);

        // The transport stays polled while the application waits for disk I/O or a retry.
        let transport = async {
            loop {
                if let Event::Incoming(packet @ (Packet::SubAck(_) | Packet::Publish(_))) =
                    event_loop.poll().await?
                    && send.send(packet).await.is_err()
                {
                    return Ok(());
                }
            }
        };

        let application = async {
            client.subscribe(topic, QOS).await?;
            match time::timeout(Duration::from_secs(8), events.recv()).await? {
                Some(Packet::SubAck(ack))
                    if ack.return_codes == [SubscribeReasonCode::Success(QOS)] => {}
                _ => return Err(format!("MQTT QoS 1 subscription failed for {topic}").into()),
            }
            ready.store(true, Ordering::Relaxed);
            eprintln!("{} subscribed to {topic}", self.role.name());

            match self.role {
                Role::Gateway => {
                    gateway(db, client, events, &telemetry_topic, &receipt_topic).await
                }
                Role::Receiver => {
                    receiver(db, client, events, &telemetry_topic, &receipt_topic).await
                }
            }
        };

        tokio::select! {
            result = transport => result,
            result = application => result,
        }
    }
}

async fn gateway(
    db: &Store,
    client: AsyncClient,
    mut events: mpsc::Receiver<Packet>,
    telemetry_topic: &str,
    receipt_topic: &str,
) -> Result<()> {
    let mut tick = time::interval(Duration::from_millis(50));
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    let mut in_flight = HashSet::with_capacity(WINDOW);
    let mut resend_at = time::Instant::now();

    loop {
        tokio::select! {
            packet = events.recv() => {
                match packet {
                    Some(Packet::Publish(message)) if message.topic == receipt_topic && !message.retain => {
                        if let Ok(receipt) = serde_json::from_slice::<Receipt>(&message.payload) {
                            let id = receipt.id.clone();
                            if db.confirm(receipt).await? {
                                in_flight.remove(&id);
                            }
                        }
                    }
                    None => return Err("MQTT transport stopped".into()),
                    _ => {}
                }
            }
            _ = tick.tick(), if in_flight.is_empty() => {
                for event in db.pending(WINDOW as i64).await? {
                    client.try_publish(telemetry_topic, QOS, false, event.payload)?;
                    in_flight.insert(event.id);
                }
                resend_at = time::Instant::now() + RETRY_RECEIPT;
            }
            _ = time::sleep_until(resend_at), if !in_flight.is_empty() => {
                in_flight.clear();
            }
        }
    }
}

async fn receiver(
    db: &Store,
    client: AsyncClient,
    mut events: mpsc::Receiver<Packet>,
    telemetry_topic: &str,
    receipt_topic: &str,
) -> Result<()> {
    while let Some(packet) = events.recv().await {
        let Packet::Publish(message) = packet else {
            continue;
        };
        if message.topic != telemetry_topic || message.retain {
            continue;
        }
        let Ok(payload) = std::str::from_utf8(&message.payload) else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Telemetry>(payload) else {
            continue;
        };
        if event.validate().is_err() {
            continue;
        }

        let receipt = Receipt {
            id: event.id,
            fingerprint: fingerprint(payload),
        };
        if db
            .record(
                receipt.id.clone(),
                payload.into(),
                receipt.fingerprint.clone(),
            )
            .await?
        {
            client.try_publish(receipt_topic, QOS, false, serde_json::to_vec(&receipt)?)?;
        } else {
            eprintln!("receiver rejected conflicting telemetry for {}", receipt.id);
        }
    }
    Err("MQTT transport stopped".into())
}
