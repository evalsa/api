use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

use axum::{
    body::Bytes,
    extract::{ws::Message, Path, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing, Router,
};
use evalsa_worker_proto::{Run, Running, RunningState};
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection,
};
use serde::Deserialize;
use tokio::{select, sync::mpsc};

#[derive(Clone)]
struct AppState {
    run_id: Arc<AtomicU64>,
    run_sender: mpsc::UnboundedSender<Run>,
    run_subscribers: Arc<Mutex<HashMap<u64, mpsc::UnboundedReceiver<Running>>>>,
}

#[tokio::main]
async fn main() {
    let (run_sender, run_receiver) = mpsc::unbounded_channel();
    let run_subscribers = Arc::new(Mutex::new(HashMap::new()));
    let state = AppState {
        run_id: Arc::new(AtomicU64::new(0)),
        run_sender,
        run_subscribers: run_subscribers.clone(),
    };
    tokio::task::spawn(poll(run_receiver, run_subscribers));
    let app = Router::new()
        .route("/run", routing::post(post_run))
        .route("/notify/:id", routing::get(get_notify))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:4000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
struct RunBody {
    language: String,
    code: Vec<u8>,
    stdin: Vec<u8>,
}

async fn post_run(State(state): State<AppState>, body: Bytes) -> String {
    let run_body: RunBody = ciborium::from_reader(&*body).unwrap();
    let id = state
        .run_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    state
        .run_sender
        .send(Run {
            id,
            language: run_body.language,
            code: run_body.code,
            stdin: run_body.stdin,
        })
        .unwrap();
    id.to_string()
}

async fn get_notify(
    State(state): State<AppState>,
    Path(id): Path<u64>,
    ws: WebSocketUpgrade,
) -> Response {
    let mut subscribers = state.run_subscribers.lock().unwrap();
    if let Some(mut subscriber) = subscribers.remove(&id) {
        ws.on_upgrade(move |mut websocket| async move {
            while let Some(event) = subscriber.recv().await {
                let mut buffer = vec![];
                ciborium::into_writer(&event, &mut buffer).unwrap();
                websocket.send(Message::Binary(buffer)).await.unwrap();
            }
        })
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn poll(
    mut task: mpsc::UnboundedReceiver<Run>,
    subscribers: Arc<Mutex<HashMap<u64, mpsc::UnboundedReceiver<Running>>>>,
) {
    let connection = Connection::connect(
        "amqp://localhost:5672",
        lapin::ConnectionProperties::default(),
    )
    .await
    .unwrap();
    let channel = connection.create_channel().await.unwrap();
    channel
        .queue_declare(
            "apibound",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    channel
        .queue_declare(
            "workerbound",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut consumer = channel
        .basic_consume(
            "apibound",
            "api",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut notifiers = HashMap::<u64, mpsc::UnboundedSender<Running>>::new();
    loop {
        select! {
            Some(running) = consumer.next() => {
                let running = running.unwrap();
                running.ack(BasicAckOptions::default()).await.unwrap();
                let data: Running = ciborium::from_reader(running.data.as_slice()).unwrap();
                if let std::collections::hash_map::Entry::Occupied(entry) = notifiers.entry(data.id) {
                    let is_finished = matches!(data.state, RunningState::Finished(_));
                    entry.get().send(data).unwrap();
                    if is_finished {
                        entry.remove();
                    }
                }
            }
            Some(run) = task.recv() => {
                let mut workerbound = vec![];
                ciborium::into_writer(&run, &mut workerbound).unwrap();
                channel
                    .basic_publish(
                        "",
                        "workerbound",
                        BasicPublishOptions::default(),
                        &workerbound,
                        BasicProperties::default(),
                    )
                    .await
                    .unwrap()
                    .await
                    .unwrap();
                let (notifier, subscriber) = mpsc::unbounded_channel();
                notifiers.insert(run.id, notifier);
                subscribers.lock().unwrap().insert(run.id, subscriber);
            }
            else => break
        }
    }
}
