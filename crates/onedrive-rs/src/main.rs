use std::env;
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use onedrive_core::Config;
use onedrive_graph::{Auth, GraphDrive};
use onedrive_rs::daemon::{self, mount_path, runtime_dir};
use onedrive_rs::ipc::{IpcClient, MountPath, Request, Response, Status, Upload, socket_path};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(version, about = "Control the OneDrive user daemon")]
struct Cli {
    /// Print a newline-delimited JSON response
    #[arg(long, global = true)]
    json: bool,

    /// Override the mount path (defaults to $XDG_RUNTIME_DIR/onedrive)
    #[arg(long, global = true, value_name = "DIRECTORY")]
    mount_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(flatten)]
    Local(LocalCommand),
    #[command(flatten)]
    Control(ControlCommand),
}

#[derive(Subcommand)]
enum ControlCommand {
    /// Show sync, cache, and upload status
    Status,
    /// Pin a file or folder
    Pin { path: PathBuf },
    /// Unpin a file or folder
    Unpin { path: PathBuf },
    /// Purge the cached contents of a file or folder
    Purge { path: PathBuf },
    /// Run a delta sync now
    Sync,
    /// Show pending uploads and conflicts
    Uploads,
    /// Pause background sync
    Pause,
    /// Resume background sync
    Resume,
}

#[derive(Subcommand)]
enum LocalCommand {
    /// Manage Microsoft OneDrive authentication
    Auth {
        #[command(subcommand)]
        action: AuthAction,
    },
    /// Run the OneDrive daemon in the foreground
    Daemon,
}

#[derive(Subcommand)]
enum AuthAction {
    /// Sign in to OneDrive
    Login,
    /// Show the authentication status
    Status,
    /// Sign out of OneDrive
    Logout,
}

fn main() -> ExitCode {
    let args: Vec<_> = env::args_os().collect();
    let json = args
        .iter()
        .skip(1)
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "--json");
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) if json && error.use_stderr() => {
            print_error(true, error.to_string().trim_end());
            return ExitCode::from(2);
        }
        Err(error) => error.exit(),
    };

    match cli.execute() {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            print_error(cli.json, &error);
            ExitCode::FAILURE
        }
    }
}

impl Cli {
    fn execute(&self) -> Result<String, String> {
        let command = match &self.command {
            Command::Local(command) => return self.run_local_command(command),
            Command::Control(command) => command,
        };
        let runtime_dir = runtime_dir()?;
        let path = |input: &Path| {
            let config = Config::load().map_err(|error| error.to_string())?;
            let mount = mount_path(&runtime_dir, &config, self.mount_path.as_deref())?;
            mount_relative_path(&mount, input)
        };
        let request = match command {
            ControlCommand::Status => Request::Status {},
            ControlCommand::Pin { path: input } => Request::Pin { path: path(input)? },
            ControlCommand::Unpin { path: input } => Request::Unpin { path: path(input)? },
            ControlCommand::Purge { path: input } => Request::Purge { path: path(input)? },
            ControlCommand::Sync => Request::Sync {},
            ControlCommand::Uploads => Request::Uploads {},
            ControlCommand::Pause => Request::Pause {},
            ControlCommand::Resume => Request::Resume {},
        };
        let result = daemon_request(&runtime_dir, &request)?;
        if self.json {
            return Ok(Response::Success(result).into_json().to_string());
        }
        render_reply(request, result)
    }

    fn run_local_command(&self, command: &LocalCommand) -> Result<String, String> {
        match command {
            LocalCommand::Auth { action } => tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("Cannot initialize OneDrive authentication ({error})"))?
                .block_on(self.run_auth(action)),
            LocalCommand::Daemon => {
                let config = Config::load().map_err(|error| error.to_string())?;
                let auth = Arc::new(Auth::new(&config).map_err(|error| error.to_string())?);
                let cloud = Arc::new(GraphDrive::new(auth).map_err(|error| error.to_string())?);
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("Cannot initialize the OneDrive daemon ({error})"))?
                    .block_on(daemon::run(config, self.mount_path.as_deref(), cloud))?;
                Ok(if self.json {
                    Response::Success(Value::Null).into_json().to_string()
                } else {
                    "OneDrive daemon stopped".into()
                })
            }
        }
    }

    async fn run_auth(&self, action: &AuthAction) -> Result<String, String> {
        let config = Config::load().map_err(|error| error.to_string())?;
        let auth = Auth::new(&config).map_err(|error| error.to_string())?;
        let signed_in = match action {
            AuthAction::Login => auth.login().await.map(|()| true),
            AuthAction::Status => auth.is_signed_in().await,
            AuthAction::Logout => auth.logout().await.map(|()| false),
        }
        .map_err(|error| error.to_string())?;
        if self.json {
            return Ok(Response::Success(json!({"signed_in": signed_in}))
                .into_json()
                .to_string());
        }
        Ok(if signed_in {
            "Signed in to OneDrive"
        } else {
            "Signed out of OneDrive"
        }
        .into())
    }
}

fn mount_relative_path(mount: &Path, input: &Path) -> Result<MountPath, String> {
    if !mount.is_absolute() {
        return Err("The OneDrive mount path must be absolute".into());
    }
    if input.as_os_str().is_empty() {
        return Err("An item path is required".into());
    }
    let absolute = if input.is_absolute() {
        input.to_path_buf()
    } else {
        env::current_dir()
            .map_err(|error| format!("Cannot determine the current directory ({error})"))?
            .join(input)
    };
    if mount
        .components()
        .chain(absolute.components())
        .any(|component| component == Component::ParentDir)
    {
        return Err("OneDrive paths cannot contain parent components (..)".into());
    }
    let relative = absolute
        .strip_prefix(mount)
        .map_err(|_| format!("Path {absolute:?} is outside the OneDrive mount at {mount:?}"))?;
    let mount_path = Path::new("/").join(relative.components().collect::<PathBuf>());
    let utf8 = mount_path.to_str().ok_or("OneDrive paths must be UTF-8")?;
    MountPath::try_from(utf8.to_owned()).map_err(str::to_owned)
}

fn daemon_request(runtime_dir: &Path, request: &Request) -> Result<Value, String> {
    let path = socket_path(runtime_dir);
    match IpcClient::new(&path).request(request) {
        Ok(Response::Success(result)) => Ok(result),
        Ok(Response::Error(error)) => Err(error),
        Err(error) => Err(format!(
            "Cannot communicate with the OneDrive daemon at {path:?} ({error})"
        )),
    }
}

fn render_reply(request: Request, result: Value) -> Result<String, String> {
    let output = match request {
        Request::Status {} => return render_status(result),
        Request::Uploads {} => return render_uploads(result),
        Request::Pin { path } => format!("Pin completed for {:?}", path.as_str()),
        Request::Unpin { path } => format!("Unpin completed for {:?}", path.as_str()),
        Request::Purge { path } => format!("Purge completed for {:?}", path.as_str()),
        Request::Sync {} => "Sync completed".into(),
        Request::Pause {} => "Paused background sync".into(),
        Request::Resume {} => "Resumed background sync".into(),
        Request::DesktopChanges { .. } => unreachable!("desktop-only IPC request"),
    };
    Ok(output)
}

fn render_uploads(result: Value) -> Result<String, String> {
    let uploads: Vec<Upload> = serde_json::from_value(result)
        .map_err(|error| format!("Invalid daemon uploads response ({error})"))?;
    if uploads.is_empty() {
        return Ok("No pending uploads or conflicts.".into());
    }
    let lines: Vec<_> = uploads
        .iter()
        .map(|upload| format!("{}  {:?}", upload.sync_state, upload.path.as_str()))
        .collect();
    Ok(lines.join("\n"))
}

fn render_status(result: Value) -> Result<String, String> {
    let status: Status = serde_json::from_value(result)
        .map_err(|error| format!("Invalid daemon status response ({error})"))?;
    let warning = if status.free_disk_floor_warning {
        "\nFree disk is below the configured floor."
    } else {
        ""
    };

    Ok(format!(
        "Sync state       {}\nUnpinned cache   {} bytes\nPinned cache     {} bytes\nPending uploads  {}\nPaused           {}{warning}",
        status.sync_state,
        status.unpinned_cache_bytes,
        status.pinned_cache_bytes,
        status.pending_upload_count,
        if status.paused { "yes" } else { "no" }
    ))
}

fn print_error(json: bool, error: &str) {
    if json {
        println!("{}", Response::Error(error.to_owned()).into_json());
    } else {
        eprintln!("{error}");
    }
}
