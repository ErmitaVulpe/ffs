use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, Subcommand};
use ffs::prelude::*;
use indoc::indoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let Args {
        bootstrap_path,
        command,
    } = Args::parse();

    match command {
        Command::Init => {
            App::init(&bootstrap_path).await?;
        }
        Command::Backend { command } => {
            let app = App::new(&bootstrap_path).await?;
            todo!();
        }
        Command::Run { command } => {
            let app = App::new(&bootstrap_path).await?;
            match command {
                RunSubCommand::Ls { path } => {
                    let res = app.list_dir(&path.unwrap_or_default()).await;
                    println!("{res:#?}");
                }
                RunSubCommand::Mkdir { path } => todo!(),
                RunSubCommand::Upload { src, target } => todo!(),
                RunSubCommand::Rm { path } => todo!(),
            }
        }
    }

    Ok(())
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        default_value = "bootstrap.ffs",
        env = "FFS_BS_PATH",
        global = true,
        help = "Path to the bootstrap file",
        short = 'i'
    )]
    bootstrap_path: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Manage backends", short_flag = 'B')]
    Backend {
        #[command(subcommand)]
        command: BackendSubCommand,
    },
    #[command(about = "Initialize a new bootstrap", short_flag = 'I')]
    Init,
    #[command(about = "Execute a command directly on the database", short_flag = 'R')]
    Run {
        #[command(subcommand)]
        command: RunSubCommand,
    },
}

#[derive(Debug, Subcommand)]
enum BackendSubCommand {
    #[command(about = "Add a new backend", short_flag = 'a')]
    Add {
        #[arg(
            value_parser = backend_kind_parser,
            long_help = indoc! {"
                Supported backends:
                google - Google drive
            "}
        )]
        kind: BackendKindSpecifier,
    },
    #[command(about = "Show currently added backends", short_flag = 'l')]
    List,
}

fn backend_kind_parser(s: &str) -> anyhow::Result<BackendKindSpecifier> {
    s.parse()
        .context("Unsupported backend. For a list of supported backends try -Ba --help")
}

#[derive(Debug, Subcommand)]
enum RunSubCommand {
    #[command(about = "List directory contents", short_flag = 'l')]
    Ls { path: Option<InodePath> },
    #[command(about = "Create a new directory", short_flag = 'm')]
    Mkdir { path: InodePath },
    #[command(about = "Uploads a new file into ffs", short_flag = 'u')]
    Upload {
        #[arg(value_parser = |s: &str| {
            let p: &Path = s.as_ref();
            p.is_file()
                .then(|| p.to_path_buf())
                .ok_or("Specified src path doesn't contain a file")
        })]
        src: PathBuf,
        target: InodePath,
    },
    #[command(about = "Remove a file or directory", short_flag = 'r')]
    Rm { path: InodePath },
}
