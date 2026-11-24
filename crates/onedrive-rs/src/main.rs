use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use onedrive_rs::ipc::{IpcClient, Request, Response, Status, socket_path};
use serde_json::Value;

#[derive(Parser)]
#[command(version, about = "Control the OneDrive user daemon")]
struct Cli {
    /// Print a newline-delimited JSON response
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show sync, cache, and upload status
    Status,
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
        let runtime_dir = runtime_dir()?;
        let result = daemon_request(&runtime_dir, &Request::Status)?;
        if self.json {
            return Ok(Response::Success(result).into_json().to_string());
        }
        render_status(result)
    }
}

fn runtime_dir() -> Result<PathBuf, String> {
    env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or_else(|| "XDG_RUNTIME_DIR must be an absolute directory path".to_owned())
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
