use anyhow::{anyhow, bail, Context};
use log::{debug, error, info, trace, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::{BorrowMut, Cow};
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, WriteHalf};
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::{self, JoinHandle};
use tokio::{process, time};
use tokio_util::sync::CancellationToken;

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(target_os = "windows")]
mod mpv_platform {
    use crate::unix_timestamp;
    use std::path::PathBuf;
    use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};
    pub type Stream = NamedPipeClient;
    pub async fn connect(path: &PathBuf) -> Result<Stream, ()> {
        let opts = ClientOptions::new();
        opts.open(path).or(Err(()))
    }
    pub fn generate_ipc_path() -> PathBuf {
        format!("\\\\.\\pipe\\mpv_ipc_{}", unix_timestamp()).into()
    }
    pub fn default_mpv_bin() -> PathBuf {
        "mpv.exe".into()
    }
}
#[cfg(not(target_os = "windows"))]
mod mpv_platform {
    use crate::unix_timestamp;
    use std::path::PathBuf;
    use tokio::net::UnixStream;
    pub type Stream = UnixStream;
    pub async fn connect(path: &PathBuf) -> Result<Stream, ()> {
        UnixStream::connect(&path).await.or(Err(()))
    }
    pub fn generate_ipc_path() -> PathBuf {
        let dir = std::env::temp_dir();
        dir.join(format!("mpv_ipc_{}.sock", unix_timestamp()))
    }
    pub fn default_mpv_bin() -> PathBuf {
        "mpv".into()
    }
}

#[derive(Serialize, Deserialize)]
struct MpvCommand {
    request_id: usize,
    command: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
struct MpvResponse {
    request_id: usize,
    data: MpvDataOption,
    error: String,
}

type LockedMpvIdMap<T> = Arc<Mutex<HashMap<usize, T>>>;
type MpvDataOption = Option<serde_json::Value>;

#[derive(Clone)]
pub enum StdoutMode {
    Inherit,
    Log,
    None,
}

#[derive(Clone)]
pub struct MpvSpawnOptions {
    pub mpv_path: Option<PathBuf>,
    pub mpv_args: Option<Vec<String>>,
    pub ipc_path: Option<PathBuf>,
    pub config_dir: Option<PathBuf>,
    pub stdout_mode: StdoutMode,
}
impl Default for MpvSpawnOptions {
    fn default() -> Self {
        Self {
            mpv_path: None,
            mpv_args: None,
            ipc_path: None,
            config_dir: None,
            stdout_mode: StdoutMode::None,
        }
    }
}

pub struct MpvIpc {
    shutdown: CancellationToken,
    writer: WriteHalf<mpv_platform::Stream>,
    request_id: usize,
    requests: LockedMpvIdMap<oneshot::Sender<anyhow::Result<serde_json::Value>>>,
    event_handlers: Arc<Mutex<HashMap<String, Vec<mpsc::Sender<serde_json::Value>>>>>,
    tasks: Vec<JoinHandle<()>>,
    kill_mpv: Option<Sender<()>>,
}
impl MpvIpc {
    /// Attach to an existing mpv IPC socket.
    pub async fn connect(ipc_path: &PathBuf) -> anyhow::Result<Self> {
        // Retry before giving up
        let (mut line_reader, writer): (Lines<_>, WriteHalf<_>) = async {
            for n in 0..10 {
                if n > 0 {
                    time::sleep(Duration::from_millis(100) * n).await;
                }
                if let Ok(stream) = mpv_platform::connect(ipc_path).await {
                    debug!("Connected to mpv socket");
                    let (reader, writer) = io::split(stream);
                    let line_reader = BufReader::new(reader).lines();
                    return Ok((line_reader, writer));
                }
            }
            bail!("failed to connect to mpv socket");
        }
        .await?;

        let requests = Arc::new(Mutex::new(HashMap::<
            usize,
            oneshot::Sender<anyhow::Result<serde_json::Value>>,
        >::new()));
        let event_handlers = Arc::new(Mutex::new(
            HashMap::<String, Vec<mpsc::Sender<serde_json::Value>>>::new(),
        ));
        let shutdown = CancellationToken::new();

        let shutdown_ref = shutdown.clone();
        let requests_ref = requests.clone();
        let event_handlers_ref = event_handlers.clone();
        let mpv_ipc_task = tokio::spawn(async move {
            loop {
                let res = tokio::select! {
                    line = line_reader.next_line() => { line },
                    _ = shutdown_ref.cancelled() => {
                        trace!("Shutdown cancellation. Breaking main loop.");
                        break;
                    }
                };
                let Ok(Some(str)) = res else {
                    warn!("Failed to read from mpv IPC. Assuming mpv shutdown.");
                    shutdown_ref.cancel();
                    // TODO: this should also abort tasks etc

                    // Send faked shutdown event to any listeners
                    if let Some(list) = event_handlers_ref.lock().await.get("shutdown") {
                        for handler in list {
                            handler.send(json!({"event": "shutdown"})).await.unwrap();
                        }
                    }
                    break; // stop main loop
                };

                trace!("<-mpv: {}", str);
                let json = serde_json::from_str::<serde_json::Value>(str.as_str()).unwrap();
                if let Ok(mpv_resp) = MpvResponse::deserialize(&json) {
                    if let Some(tx) = requests_ref.lock().await.remove(&mpv_resp.request_id) {
                        if mpv_resp.error == "success" {
                            tx.send(Ok(mpv_resp.data.unwrap_or(serde_json::Value::Null))).unwrap();
                        } else {
                            tx.send(Err(anyhow!(mpv_resp.error))).unwrap();
                        }
                    } else {
                        warn!("Unhandled requests ID: {}", mpv_resp.request_id);
                    }
                } else if let Some(event) = json.as_object().and_then(|j| j.get("event")).and_then(|j| j.as_str()) {
                    trace!("Event '{}'", event);
                    if let Some(list) = event_handlers_ref.lock().await.get(event) {
                        for handler in list {
                            handler.send(json.clone()).await.unwrap();
                        }
                    }
                    if event == "shutdown" {
                        info!("Received mpv 'shutdown' event.");
                        shutdown_ref.cancel();
                        // TODO: this should also abort tasks etc
                        break; // stop main loop
                    }
                } else {
                    warn!("Unhandled mpv message: {}", str);
                }
            }
        });

        Ok(Self {
            shutdown,
            writer,
            request_id: 0,
            requests,
            event_handlers,
            tasks: vec![mpv_ipc_task],
            kill_mpv: None,
        })
    }
    /// Spawn a new mpv process and attach to it.
    pub async fn spawn(opt: &MpvSpawnOptions) -> anyhow::Result<Self> {
        let mpv_path = opt
            .mpv_path
            .as_ref()
            .map(|v| Cow::Borrowed(v))
            .unwrap_or_else(|| Cow::Owned(mpv_platform::default_mpv_bin()));
        let ipc_path = opt
            .ipc_path
            .as_ref()
            .map(|v| Cow::Borrowed(v))
            .unwrap_or_else(|| Cow::Owned(mpv_platform::generate_ipc_path()));
        let mut args = vec![
            "--idle".to_owned(),
            "--input-ipc-server=".to_owned() + &ipc_path.to_string_lossy(),
        ];
        if let Some(extra_args) = &opt.mpv_args {
            args.extend(extra_args.clone());
        }
        if let Some(config_dir) = &opt.config_dir {
            args.push("--config-dir=".to_owned() + &config_dir.to_string_lossy());
        }
        let stdout_mode = || match opt.stdout_mode {
            StdoutMode::Inherit => Stdio::inherit(),
            StdoutMode::Log => Stdio::piped(),
            StdoutMode::None => Stdio::null(),
        };
        debug!("mpv args: {}", args.join(" "));
        let mut child = process::Command::new(mpv_path.as_ref())
            .args(args)
            .stdin(Stdio::null())
            .stdout(stdout_mode())
            .stderr(stdout_mode())
            .spawn()
            .context("Failed to spawn mpv process")?;
        let child_pid = child.id().unwrap();
        info!("mpv spawned! pid: {}", child_pid);

        let (kill_send, kill_recv) = oneshot::channel::<()>();

        let mut child_tasks = vec![];
        if matches!(opt.stdout_mode, StdoutMode::Log) {
            let stdout = child.stdout.take().expect("Could not capture stdout for mpv");
            child_tasks.push(task::spawn(async {
                let mut stdout = BufReader::new(stdout).lines();
                while let Some(line) = stdout.next_line().await.unwrap_or_else(|e| Some(e.to_string())) {
                    if !line.trim().is_empty() {
                        debug!("mpv(out): {line}");
                    }
                }
            }));
            let stderr = child.stderr.take().expect("Could not capture stderr for mpv");
            child_tasks.push(task::spawn(async {
                let mut stderr = BufReader::new(stderr).lines();
                while let Some(line) = stderr.next_line().await.unwrap_or_else(|e| Some(e.to_string())) {
                    if !line.trim().is_empty() {
                        error!("mpv(err): {line}");
                    }
                }
            }));
        }
        child_tasks.push(task::spawn(async move {
            if kill_recv.await.is_err() {
                error!("mpv kill handle was dropped");
            }
        }));

        // Connect
        let mut sself = Self::connect(&ipc_path).await?;
        sself.tasks.append(&mut child_tasks);
        sself.kill_mpv = Some(kill_send);

        // Sanity check
        let ipc_pid = sself.get_prop::<u32>("pid").await?;
        if ipc_pid != child_pid {
            warn!("mpv process pid and mpv ipc pid don't match");
        }

        Ok(sself)
    }
    pub async fn running(&self) -> bool {
        !self.shutdown.is_cancelled()
    }
    /// Send a command to mpv and wait for a reply.
    /// This should not be used to `quit` because it will never receive a reply. Use the `quit` function instead.
    pub async fn send_command(&mut self, cmd: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        if self.shutdown.is_cancelled() {
            bail!("mpv instance has shut down");
        }
        let (tx, rx) = oneshot::channel::<anyhow::Result<serde_json::Value>>();
        self.request_id += 1;
        self.requests.lock().await.insert(self.request_id, tx);
        let str = serde_json::to_string(&MpvCommand {
            request_id: self.request_id,
            command: cmd,
        })
        .unwrap();
        trace!("->mpv: {}", str);
        self.writer.write_all((str + "\n").as_bytes()).await?;
        tokio::select! {
            result = rx => result?,
            _ = self.shutdown.cancelled() => bail!("mpv shutdown"),
        }
    }
    fn abort_tasks(&mut self) {
        for handle in &self.tasks {
            handle.abort();
        }
        self.tasks.clear();
    }
    /// Shuts down the mpv player and disconnects.
    pub async fn quit(&mut self) {
        self.abort_tasks();
        let quit_fut = self.writer.write_all(("{\"command\":[\"quit\"]}\n").as_bytes());
        _ = tokio::time::timeout(Duration::from_secs(2), quit_fut).await;
        _ = self.writer.shutdown().await;
        if let Some(kill_mpv) = self.kill_mpv.take() {
            _ = kill_mpv.send(());
        }
        self.shutdown.cancel();
    }
    /// Disconnect from the IPC socket.
    pub async fn disconnect(&mut self) {
        self.abort_tasks();
        _ = self.writer.shutdown().await;
        self.shutdown.cancel();
    }
    pub async fn get_prop<T: DeserializeOwned>(&mut self, name: &str) -> anyhow::Result<T> {
        self.send_command(json!(["get_property", name]))
            .await
            .and_then(|json| T::deserialize(json).map_err(|_| anyhow!("failed to deserialize prop")))
    }
    pub async fn set_prop(&mut self, name: &str, value: impl Serialize) -> anyhow::Result<()> {
        self.send_command(json!(["set_property", name, value]))
            .await
            .map(|_| ())
    }
    pub async fn watch_event<A, F, Fut>(
        &mut self,
        name: impl AsRef<str> + 'static + Send + Sync + Serialize + Display,
        callback: F,
    ) where
        for<'a> Fut: Future<Output = A> + Send + 'a,
        for<'a> F: (Fn(serde_json::Value) -> Fut) + Send + 'a,
    {
        let (json_tx, mut json_rx) = mpsc::channel::<serde_json::Value>(1);
        let enable = {
            let mut event_handlers = self.borrow_mut().event_handlers.lock().await;
            if let Some(list) = event_handlers.get_mut(name.as_ref()) {
                list.push(json_tx);
                false
            } else {
                _ = event_handlers.insert(name.to_string(), vec![json_tx]);
                true
            }
        };
        if enable {
            self.send_command(json!(["enable_event", name])).await.unwrap();
        }
        self.tasks.push(tokio::spawn(async move {
            loop {
                let json = json_rx.recv().await.unwrap();
                trace!("Got watched event value '{}': {:?}", name, json);
                callback(json).await;
            }
        }));
    }

    /// Returns the request ID for the observed property so it can be unobserved
    pub async fn observe_prop(
        &mut self,
        name: impl AsRef<str> + 'static + Send + Sync + Serialize + Display,
    ) -> anyhow::Result<usize> {
        // Create observer
        self.request_id += 1;
        let id = self.request_id;
        self.send_command(json!(["observe_property", id, name])).await.unwrap();
        Ok(id)
    }
}
impl Drop for MpvIpc {
    fn drop(&mut self) {
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async {
                if self.kill_mpv.is_some() {
                    self.quit().await;
                } else {
                    self.disconnect().await;
                }
            });
        });
    }
}
