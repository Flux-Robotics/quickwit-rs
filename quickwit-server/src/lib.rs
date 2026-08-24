//! Utilities for running an ephemeral Quickwit node in tests.
//!
//! The helpers in this module are intended for integration tests. A node is
//! started with a private configuration file and is stopped automatically when
//! its [`Server`] is dropped.

use std::fmt;
use std::fs;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// A running, disposable Quickwit node.
///
/// The process is terminated and its temporary configuration is removed on
/// drop. The node's REST endpoint is available through [`Server::rest_url`].
pub struct Server {
    child: Child,
    _config_dir: tempfile::TempDir,
    rest_port: u16,
}

impl Server {
    /// Start a node using the default Quickwit executable.
    pub fn start(config: &str) -> Result<Self, Error> {
        Builder::new().config(config).start()
    }

    /// The port on which the REST API is listening.
    pub fn rest_port(&self) -> u16 {
        self.rest_port
    }

    /// Alias for [`Server::rest_port`].
    pub fn port(&self) -> u16 {
        self.rest_port()
    }

    /// The base URL for the REST API.
    pub fn rest_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.rest_port)
    }

    /// Alias for [`Server::rest_url`].
    pub fn url(&self) -> String {
        self.rest_url()
    }

    /// The process ID of the running node.
    pub fn id(&self) -> u32 {
        self.child.id()
    }

    /// Returns whether the Quickwit process is still running.
    pub fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Configuration for starting an ephemeral Quickwit node.
#[derive(Debug, Clone)]
pub struct Builder {
    binary: Option<PathBuf>,
    config: String,
    startup_timeout: Duration,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            binary: None,
            config: String::new(),
            startup_timeout: Duration::from_secs(15),
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Quickwit YAML configuration. It is written to a temporary file.
    pub fn config(mut self, config: impl Into<String>) -> Self {
        self.config = config.into();
        self
    }

    /// Use a particular Quickwit executable instead of searching for one.
    pub fn binary(mut self, binary: impl Into<PathBuf>) -> Self {
        self.binary = Some(binary.into());
        self
    }

    /// Set how long [`Builder::start`] waits for the REST socket to open.
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    pub fn start(self) -> Result<Server, Error> {
        let binary = self.binary.unwrap_or_else(default_binary);
        let ports = allocate_ports()?;
        let config_dir = tempfile::Builder::new()
            .prefix("quickwit-test-")
            .tempdir()?;
        let config_path = config_dir.path().join("quickwit.yaml");
        let data_dir = config_dir.path().join("data");
        fs::create_dir_all(&data_dir)?;
        let config = make_config(&self.config, ports, config_dir.path())?;
        fs::write(&config_path, config)?;

        let mut child = Command::new(&binary)
            .arg("run")
            .arg("--config")
            .arg(&config_path)
            .arg("--yes")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|source| Error::Spawn { binary, source })?;

        let deadline = Instant::now() + self.startup_timeout;
        loop {
            if TcpStream::connect(("127.0.0.1", ports[0])).is_ok() {
                return Ok(Server {
                    child,
                    _config_dir: config_dir,
                    rest_port: ports[0],
                });
            }
            if let Some(status) = child.try_wait()? {
                return Err(Error::Exited(status));
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::StartupTimeout(self.startup_timeout));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }
}

/// Start a disposable node with the supplied YAML configuration.
pub fn run_server(config: &str) -> Server {
    Server::start(config).unwrap_or_else(|error| panic!("failed to start Quickwit: {error}"))
}

fn default_binary() -> PathBuf {
    if let Some(path) = std::env::var_os("QUICKWIT_BIN") {
        return PathBuf::from(path);
    }

    PathBuf::from("quickwit")
}

fn allocate_ports() -> io::Result<[u16; 3]> {
    // Binding one listener and taking its port avoids collisions between
    // parallel test processes as far as the OS can provide. The adjacent
    // ports are reserved too, since Quickwit uses REST, gRPC, and gossip.
    for _ in 0..100 {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let first = listener.local_addr()?.port();
        if first < u16::MAX - 2
            && TcpListener::bind(("127.0.0.1", first + 1)).is_ok()
            && TcpListener::bind(("127.0.0.1", first + 2)).is_ok()
        {
            drop(listener);
            return Ok([first, first + 1, first + 2]);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AddrInUse,
        "could not allocate Quickwit ports",
    ))
}

fn make_config(config: &str, ports: [u16; 3], temp_dir: &Path) -> Result<String, Error> {
    let mut value: serde_yaml::Value = if config.trim().is_empty() {
        serde_yaml::from_str("version: 0.8")?
    } else {
        serde_yaml::from_str(config)?
    };
    let map = value.as_mapping_mut().ok_or(Error::InvalidConfig)?;
    map.entry(serde_yaml::Value::String("version".into()))
        .or_insert(serde_yaml::Value::String("0.8".into()));
    map.insert(
        serde_yaml::Value::String("listen_address".into()),
        serde_yaml::Value::String("127.0.0.1".into()),
    );
    let rest = map
        .entry(serde_yaml::Value::String("rest".into()))
        .or_insert_with(|| serde_yaml::Value::Mapping(Default::default()));
    rest.as_mapping_mut().ok_or(Error::InvalidConfig)?.insert(
        serde_yaml::Value::String("listen_port".into()),
        serde_yaml::Value::Number(ports[0].into()),
    );
    map.entry(serde_yaml::Value::String("data_dir".into()))
        .or_insert_with(|| serde_yaml::Value::String(temp_dir.join("data").display().to_string()));
    // Quickwit derives these service ports from the REST port unless supplied.
    Ok(serde_yaml::to_string(&value)?)
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Spawn { binary: PathBuf, source: io::Error },
    Yaml(serde_yaml::Error),
    InvalidConfig,
    Exited(std::process::ExitStatus),
    StartupTimeout(Duration),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<serde_yaml::Error> for Error {
    fn from(error: serde_yaml::Error) -> Self {
        Self::Yaml(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Spawn { binary, source } => {
                write!(f, "could not execute {}: {source}", binary.display())
            }
            Self::Yaml(e) => write!(f, "invalid Quickwit YAML: {e}"),
            Self::InvalidConfig => write!(f, "Quickwit configuration must be a YAML mapping"),
            Self::Exited(status) => write!(f, "Quickwit exited during startup with {status}"),
            Self::StartupTimeout(timeout) => write!(f, "Quickwit did not start within {timeout:?}"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_local_quickwit() {
        let server = Builder::new().config("version: 0.8").start().unwrap();
        assert!(server.rest_url().starts_with("http://127.0.0.1:"));
    }

    #[test]
    fn config_preserves_user_values_and_sets_test_ports() {
        let config = make_config(
            "version: 0.8\nrest:\n  cors_allow_origins: [http://example.test]\n",
            [1234, 1235, 1236],
            Path::new("/tmp/qw"),
        )
        .unwrap();
        assert!(config.contains("listen_port: 1234"));
        assert!(config.contains("cors_allow_origins"));
        assert!(config.contains("listen_address: 127.0.0.1"));
    }
}
