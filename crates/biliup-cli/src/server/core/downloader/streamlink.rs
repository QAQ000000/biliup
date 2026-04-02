use crate::server::errors::{AppError, AppResult};
use error_stack::ResultExt;
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::RwLock;
use tokio::time::Duration;
use tracing::{error, info, warn};
use url::Url;

#[derive(Debug, Clone)]
pub enum Platform {
    Bilibili,
    Twitch { disable_ads: bool },
    Generic,
}

#[derive(Debug, Clone)]
pub enum OutputMode {
    Pipe,
    HttpServer { port: u16 },
}

pub struct Streamlink {
    streamlink_downloader: StreamlinkDownloader,
    process_handle: Arc<RwLock<Option<Child>>>,
}

impl Streamlink {
    pub fn new(streamlink_downloader: StreamlinkDownloader) -> Streamlink {
        Self {
            streamlink_downloader,
            process_handle: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) async fn stop(&self) -> AppResult<()> {
        warn!("Streamlink stop is currently a no-op");
        Ok(())
    }
}

pub struct StreamlinkDownloader {
    platform: Platform,
    url: String,
    headers: HashMap<String, String>,
    output_mode: OutputMode,
}

impl StreamlinkDownloader {
    pub fn new(url: String, platform: Platform) -> Self {
        Self {
            platform,
            url,
            headers: HashMap::new(),
            output_mode: OutputMode::Pipe,
        }
    }

    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    pub fn with_output_mode(mut self, mode: OutputMode) -> Self {
        self.output_mode = mode;
        self
    }

    pub fn start(&mut self) -> AppResult<StreamOutput> {
        let mut cmd = Command::new("streamlink");

        cmd.args([
            "--stream-segment-threads",
            "3",
            "--hls-playlist-reload-attempts",
            "1",
        ]);

        for (key, value) in &self.headers {
            cmd.args(["--http-header", &format!("{}={}", key, value)]);
        }

        let platform_args = self.build_platform_args()?;
        for arg in platform_args {
            cmd.arg(arg);
        }

        let output = match &self.output_mode {
            OutputMode::Pipe => {
                cmd.args([&self.url, "best", "-O"]);
                cmd.stdout(Stdio::piped());
                let child = cmd.spawn().change_context(AppError::Unknown)?;
                StreamOutput::Pipe(child)
            }
            OutputMode::HttpServer { port } => {
                cmd.args([
                    "--player-external-http",
                    "--player-external-http-port",
                    &port.to_string(),
                    "--player-external-http-interface",
                    "localhost",
                    &self.url,
                    "best",
                ]);
                let child = cmd.spawn().change_context(AppError::Unknown)?;
                StreamOutput::Http {
                    url: format!("http://localhost:{}", port),
                    process: child,
                }
            }
        };

        Ok(output)
    }

    fn build_platform_args(&self) -> AppResult<Vec<String>> {
        let mut args = Vec::new();

        match &self.platform {
            Platform::Bilibili => {
                args.extend(self.parse_bilibili_params()?);
            }
            Platform::Twitch { disable_ads } => {
                if *disable_ads {
                    args.push("--twitch-disable-ads".to_string());
                }

                if let Some(token) = Self::get_twitch_auth_token() {
                    args.push(format!("--twitch-api-header=Authorization=OAuth {}", token));
                }
            }
            Platform::Generic => {}
        }

        Ok(args)
    }

    fn parse_bilibili_params(&self) -> AppResult<Vec<String>> {
        let mut params = Vec::new();

        let url = Url::parse(&self.url).change_context(AppError::Unknown)?;
        let mut whitelist = vec![
            "uparams",
            "upsig",
            "sigparams",
            "sign",
            "flvsk",
            "sk",
            "mid",
            "site",
        ];

        let query_pairs: HashMap<_, _> = url.query_pairs().collect();
        if let Some(sigparams) = query_pairs.get("sigparams") {
            whitelist.extend(sigparams.split(',').map(|s| s.trim()));
        }
        if let Some(uparams) = query_pairs.get("uparams") {
            whitelist.extend(uparams.split(',').map(|s| s.trim()));
        }

        for (key, value) in url.query_pairs() {
            if whitelist.contains(&key.as_ref()) {
                params.push("--http-query-param".to_string());
                params.push(format!("{}={}", key, value));
            }
        }

        Ok(params)
    }

    fn get_twitch_auth_token() -> Option<String> {
        std::env::var("TWITCH_AUTH_TOKEN").ok()
    }
}

pub enum StreamOutput {
    Pipe(Child),
    Http { url: String, process: Child },
}

impl StreamOutput {
    pub async fn get_input_uri(&mut self) -> String {
        self.wait_for_startup().await;
        match self {
            StreamOutput::Pipe(_) => "pipe:0".to_string(),
            StreamOutput::Http { url, .. } => url.clone(),
        }
    }

    async fn wait_for_startup(&mut self) {
        let process = match self {
            StreamOutput::Pipe(process) => process,
            StreamOutput::Http { process, .. } => process,
        };

        match tokio::time::timeout(Duration::from_secs(5), process.wait()).await {
            Ok(code) => info!("StreamOutput exited with code {:?}", code),
            Err(e) => error!(e = ?e, "Timed out waiting for stream output"),
        }
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        match self {
            StreamOutput::Pipe(child) => child.stdout.take(),
            StreamOutput::Http { .. } => None,
        }
    }

    pub async fn stop(&mut self) {
        info!("Preparing to stop stream terminated");
        let child = match self {
            StreamOutput::Pipe(c) => c,
            StreamOutput::Http { process, .. } => process,
        };

        let _ = child.kill().await;
        let _ = child.wait().await;
        info!("Stream terminated");
    }
}
