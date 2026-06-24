use anyhow::Context;
use clap::{Parser, Subcommand};
use ffs::{App, BackendKindSpecifier, InodePath};
use indoc::indoc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Args::parse().run().await
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(
        default_value = "db.ffs",
        env = "DB_PATH",
        global = true,
        help = "Path to the database file",
        short = 'i'
    )]
    db_path: String,

    #[command(subcommand)]
    command: Command,
}

impl Args {
    async fn run(self) -> anyhow::Result<()> {
        let app = App::new(self.db_path)?;
        self.command.run(app).await
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Manage backends", short_flag = 'B')]
    Backend {
        #[command(subcommand)]
        command: BackendSubCommand,
    },
    #[command(about = "Try to compact the database file")]
    Compact,
    #[command(about = "Execute a command directly on the database", short_flag = 'R')]
    Run {
        #[command(subcommand)]
        command: RunSubCommand,
    },
}

impl Command {
    async fn run(self, mut app: App) -> anyhow::Result<()> {
        match self {
            Command::Backend { command } => command.run(app).await,
            Command::Compact => {
                app.compact_db().map_err(anyhow::Error::from)?;
                Ok(())
            }
            Command::Run { command } => command.run(app),
        }
    }
}

#[derive(Debug, Subcommand)]
enum BackendSubCommand {
    #[command(
        about = "Add a new backend",
        short_flag = 'a',
        long_about = indoc! {"
            Supported backends:
            asd - fgh
            asd - fgh
            asd - fgh
        "}
    )]
    Add {
        #[arg(value_parser = backend_kind_parser)]
        kind: BackendKindSpecifier,
    },
    #[command(about = "Show currently added backends", short_flag = 'l')]
    List,
}

fn backend_kind_parser(s: &str) -> anyhow::Result<BackendKindSpecifier> {
    s.parse()
        .context("Unsupported backend. For a list of supported backends try -Ba --help")
}

impl BackendSubCommand {
    async fn run(self, app: App) -> anyhow::Result<()> {
        match self {
            BackendSubCommand::Add { kind } => app.add_backend(kind).await,
            BackendSubCommand::List => {
                let mut table = tabled::builder::Builder::new();
                for result in app.list_backends()? {
                    let (id, meta) = result?;
                    table.push_record(
                        [
                            id.to_string(),
                            meta.free.to_string(),
                            meta.total.to_string(),
                            meta.chunks_contained.to_string(),
                            meta.kind.to_string(),
                        ]
                        .iter(),
                    );
                }

                let table = table.build();
                println!("{table}");
                Ok(())
            }
        }
    }
}

#[derive(Debug, Subcommand)]
enum RunSubCommand {
    #[command(about = "List directory contents", short_flag = 'l')]
    Ls { path: Option<InodePath> },
    #[command(about = "Create a new directory", short_flag = 'm')]
    Mkdir { path: InodePath },
    #[command(about = "Remove a file or directory", short_flag = 'r')]
    Rm { path: InodePath },
}

impl RunSubCommand {
    fn run(self, app: App) -> anyhow::Result<()> {
        match self {
            RunSubCommand::Ls { path } => {
                let path = path.unwrap_or_default();
                for child in app.read_dir(&path)? {
                    let (_, meta) = child?;
                    println!("{}", meta.name);
                }

                Ok(())
            }
            RunSubCommand::Mkdir { path } => app.mkdir(path),
            RunSubCommand::Rm { path } => app.rm(&path),
        }
    }
}
