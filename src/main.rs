use anyhow::Context;
use clap::{Parser, Subcommand};
use ffs::{App, InodeMeta, InodePath};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Args::parse().run()
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
    fn run(self) -> anyhow::Result<()> {
        let app = App::new(self.db_path)?;
        self.command.run(app)
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(about = "Try to compact the database file")]
    Compact,
    #[command(about = "Execute a command directly on the database", short_flag = 'R')]
    Run {
        #[command(subcommand)]
        command: RunSubCommand,
    },
}

impl Command {
    fn run(self, mut app: App) -> anyhow::Result<()> {
        match self {
            Command::Compact => {
                app.db.compact().map_err(anyhow::Error::from)?;
                Ok(())
            }
            Command::Run { command } => command.run(app),
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
                let inode = app.db.inode_lookup(&path)?.context("Directory not found")?;

                for child in app.db.iter_children(inode)? {
                    let (_, meta) = child?;
                    println!("{}", meta.name);
                }

                Ok(())
            }
            RunSubCommand::Mkdir { mut path } => {
                let name = path.pop().context("No directory name specified")?;
                let inode = InodeMeta::new_directory(name);
                let parent_inode = app
                    .db
                    .inode_lookup(&path)?
                    .context("Parent directory doesnt exist")?;
                app.db.create_inode(parent_inode, inode)?;
                Ok(())
            }
            RunSubCommand::Rm { path } => {
                let inode = app
                    .db
                    .inode_lookup(&path)?
                    .context("File or directory not found")?;
                app.db.remove_inode(inode)?;
                Ok(())
            }
        }
    }
}
