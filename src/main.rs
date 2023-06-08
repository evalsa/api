use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc, Mutex},
};

use axum::{
    extract::{ws, BodyStream, Path, State},
    http::StatusCode,
    response::Response,
    routing, Router,
};
use evalsa_worker_proto::{ApiBound, Finished, Run, RunResult};
use futures_util::StreamExt;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};

#[derive(Clone)]
struct AppState {
    run_id: Arc<AtomicU32>,
    finish_tx_list: Arc<Mutex<HashMap<u64, oneshot::Sender<Finished>>>>,
    finish_rx_queue: Arc<Mutex<HashMap<u64, oneshot::Receiver<Finished>>>>,
    run_tx: UnboundedSender<Run>,
}

#[tokio::main]
async fn main() {
    let (run_tx, run_rx) = unbounded_channel();
    let finish_tx_list = Arc::new(Mutex::new(HashMap::new()));
    let ftl = finish_tx_list.clone();
    tokio::task::spawn_blocking(move || poll(run_rx, ftl));
    let app = Router::new()
        .route("/run/:language/:nonce", routing::post(post_run))
        .route("/notify/:id", routing::get(get_notify))
        .with_state(AppState {
            run_id: Arc::new(AtomicU32::new(0)),
            finish_tx_list,
            finish_rx_queue: Arc::new(Mutex::new(HashMap::new())),
            run_tx,
        });
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn post_run(
    Path((language, nonce)): Path<(String, u32)>,
    State(state): State<AppState>,
    mut body: BodyStream,
) -> Result<String, StatusCode> {
    if language != "rust" {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut bytes = vec![];
    while let Some(chunk) = body.next().await {
        let Ok(chunk) = chunk else {
            return Err(StatusCode::BAD_REQUEST);
        };
        if bytes.len() + chunk.len() > 1024 * 1024 {
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        bytes.extend_from_slice(&chunk);
    }
    let Ok(code) = String::from_utf8(bytes) else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let id_upper = state
        .run_id
        .fetch_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |x| Some(x.wrapping_add(1)),
        )
        .unwrap();
    let id = (id_upper as u64) << 32 | nonce as u64;
    let (tx, rx) = oneshot::channel();
    {
        let mut finish_tx_list = state.finish_tx_list.lock().unwrap();
        finish_tx_list.insert(id, tx);
        let mut finish_rx_queue = state.finish_rx_queue.lock().unwrap();
        // TODO: Prevent queue saturation
        finish_rx_queue.insert(id, rx);
    }
    let Ok(_) = state.run_tx.send(Run { id, language, code }) else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };
    Ok(id.to_string())
}

async fn get_notify(
    Path(id): Path<u64>,
    State(state): State<AppState>,
    ws: ws::WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    if let Some(finish_rx) = state.finish_rx_queue.lock().unwrap().remove(&id) {
        Ok(ws.on_upgrade(move |mut socket: ws::WebSocket| async {
            if let Ok(finished) = finish_rx.await {
                let mut message = vec![];
                let result = match finished.result {
                    RunResult::Success => 0u8,
                    RunResult::Timeout => 1,
                    RunResult::CompileError => 2,
                    RunResult::RuntimeError => 3,
                };
                message.push(result);
                message.extend(finished.exit_code.unwrap_or(0).to_le_bytes());
                message.extend((finished.stderr.len() as u32).to_le_bytes());
                message.extend(finished.stderr);
                message.extend((finished.stdout.len() as u32).to_le_bytes());
                message.extend(finished.stdout);
                socket.send(ws::Message::Binary(message)).await.unwrap();
            }
            socket.close().await.ok();
        }))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

fn poll(mut rx: UnboundedReceiver<Run>, ids: Arc<Mutex<HashMap<u64, oneshot::Sender<Finished>>>>) {
    let ctx = zmq::Context::new();
    let socket = ctx.socket(zmq::REP).unwrap();
    socket.bind("tcp://0.0.0.0:5000").unwrap();
    let mut msg = zmq::Message::new();
    loop {
        socket.recv(&mut msg, 0).unwrap();
        let apibound: ApiBound = bincode::deserialize(msg.as_ref()).unwrap();
        match apibound {
            ApiBound::Idle { languages: _ } => {
                let Some(run) = rx.blocking_recv() else { return };
                let serialized = bincode::serialize(&run).unwrap();
                socket.send(&serialized, 0).unwrap();
            }
            ApiBound::Fetched => {
                socket.send([].as_slice(), 0).unwrap();
            }
            ApiBound::Reject { id } => {
                let mut ids = ids.lock().unwrap();
                ids.remove(&id);
                socket.send([].as_slice(), 0).unwrap();
            }
            ApiBound::Finished(finished) => {
                let mut ids = ids.lock().unwrap();
                if let Some(tx) = ids.remove(&finished.id) {
                    tx.send(finished).unwrap();
                }
                socket.send([].as_slice(), 0).unwrap();
            }
        }
    }
}
