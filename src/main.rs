use axum::{routing, Router};

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", routing::get(|| async { "Hello World!" }));
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
