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
        println!("{self:#?}");
        let app = App::new(self.db_path)?;
        self.command.run(app)
    }
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(short_flag = 'R')]
    Run {
        #[command(subcommand)]
        command: RunSubCommand,
    },
}

impl Command {
    fn run(self, app: App) -> anyhow::Result<()> {
        match self {
            Command::Run { command } => command.run(app),
        }
    }
}

#[derive(Debug, Subcommand)]
enum RunSubCommand {
    #[command(short_flag = 'l')]
    Ls { path: Option<InodePath> },
    #[command(short_flag = 'm')]
    Mkdir { path: InodePath },
    #[command(short_flag = 'r')]
    Rmdir { path: InodePath },
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
            RunSubCommand::Rmdir { path } => todo!(),
        }
    }
}
