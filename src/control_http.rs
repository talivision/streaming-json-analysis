use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

pub struct ControlReply {
    pub status: u16,
    pub body: Value,
}

pub enum ControlCommand {
    Start {
        label: Option<String>,
        reply: Sender<ControlReply>,
    },
    Stop {
        reply: Sender<ControlReply>,
    },
    Status {
        reply: Sender<ControlReply>,
    },
}

#[derive(Deserialize)]
struct StartPayload {
    label: Option<String>,
}

pub fn spawn_control_http_server(
    bind_addr: String,
    command_tx: Sender<ControlCommand>,
) -> Result<JoinHandle<()>> {
    let server = Server::http(&bind_addr)
        .map_err(|err| anyhow!("failed to bind control HTTP server on {bind_addr}: {err}"))?;
    let handle = thread::spawn(move || {
        for req in server.incoming_requests() {
            handle_request(req, &command_tx);
        }
    });
    Ok(handle)
}

fn handle_request(mut req: Request, command_tx: &Sender<ControlCommand>) {
    let method = req.method().clone();
    let path = req.url().split('?').next().unwrap_or("");

    match (method, path) {
        (Method::Post, "/action/start") => {
            let mut body = String::new();
            if req.as_reader().read_to_string(&mut body).is_err() {
                respond_json(
                    req,
                    400,
                    json!({ "ok": false, "error": "failed to read request body" }),
                );
                return;
            }
            let label = if body.trim().is_empty() {
                None
            } else {
                match serde_json::from_str::<StartPayload>(&body) {
                    Ok(payload) => payload.label.filter(|s| !s.trim().is_empty()),
                    Err(err) => {
                        respond_json(
                            req,
                            400,
                            json!({ "ok": false, "error": format!("invalid JSON payload: {err}") }),
                        );
                        return;
                    }
                }
            };
            dispatch_command(req, command_tx, |reply| ControlCommand::Start { label, reply });
        }
        (Method::Post, "/action/stop") => {
            dispatch_command(req, command_tx, |reply| ControlCommand::Stop { reply });
        }
        (Method::Get, "/action/status") => {
            dispatch_command(req, command_tx, |reply| ControlCommand::Status { reply });
        }
        _ => respond_json(
            req,
            404,
            json!({
                "ok": false,
                "error": "not found",
                "routes": ["POST /action/start", "POST /action/stop", "GET /action/status"]
            }),
        ),
    }
}

fn dispatch_command<F>(req: Request, command_tx: &Sender<ControlCommand>, build: F)
where
    F: FnOnce(Sender<ControlReply>) -> ControlCommand,
{
    let (reply_tx, reply_rx) = mpsc::channel();
    if command_tx.send(build(reply_tx)).is_err() {
        respond_json(
            req,
            503,
            json!({ "ok": false, "error": "control channel unavailable" }),
        );
        return;
    }
    match reply_rx.recv_timeout(Duration::from_secs(2)) {
        Ok(reply) => respond_json(req, reply.status, reply.body),
        Err(_) => respond_json(
            req,
            504,
            json!({ "ok": false, "error": "control command timed out" }),
        ),
    }
}

fn respond_json(req: Request, status: u16, body: Value) {
    let payload = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());
    let mut response = Response::from_string(payload).with_status_code(StatusCode(status));
    if let Ok(header) = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]) {
        response = response.with_header(header);
    }
    let _ = req.respond(response);
}
