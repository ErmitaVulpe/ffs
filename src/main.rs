use ffs_daemon::{App, EX};
use smol::{channel::unbounded, future};

mod backend;

fn main() {
    let (signal, shutdown) = unbounded::<()>();
    easy_parallel:: Parallel::new()
        .each(0..4, |_| future::block_on(EX.run(shutdown.recv())))
        .finish(|| future::block_on(async {
            task().await;
            drop(signal);
        }));
}

async fn task() {
    let app = App::load(
        "db.redb",
        "mods"
    ).await.unwrap();

    println!("{app:#?}");
    #[cfg(debug_assertions)]
    app.show_all_tables();
    println!("-----------------");
    app.add_backend("Google Drive").await.unwrap();
    println!("{app:#?}");
    #[cfg(debug_assertions)]
    app.show_all_tables();
}