use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{Context, ensure};
use p2p_chain_lib_chain::block::Block;
use p2p_chain_lib_consensus::{SimpleConsensus, validator_id_from_seed};
use p2p_chain_lib_storage::ChainStore;

#[derive(Default)]
struct Nodes {
    children: Vec<Child>,
    log_threads: Vec<JoinHandle<std::io::Result<()>>>,
}

impl Nodes {
    fn check_alive(&mut self) -> anyhow::Result<()> {
        for child in &mut self.children {
            ensure!(
                child.try_wait()?.is_none(),
                "node {} exited early",
                child.id()
            );
        }
        Ok(())
    }
}

impl Drop for Nodes {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        for thread in self.log_threads.drain(..) {
            if !matches!(thread.join(), Ok(Ok(()))) {
                eprintln!("failed to forward a node log");
            }
        }
    }
}

fn start_node(
    nodes: &mut Nodes,
    directory: &Path,
    name: &str,
    bootstrap: Option<&str>,
    produce: bool,
) -> anyhow::Result<()> {
    let log = File::create(directory.join(format!("{name}.log")))?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_p2p-chain-lib-cli"));
    // Local shell configuration must not change this test network.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("P2P_") {
            command.env_remove(key);
        }
    }
    command
        .env("RUST_LOG", "info")
        .env("NO_COLOR", "1")
        .arg("--config")
        .arg(directory.join("network.toml"))
        .args(["--listen", "/ip4/127.0.0.1/tcp/0", "--node-key-seed", name])
        .arg("--chain-path")
        .arg(directory.join(format!("{name}.bin")))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(address) = bootstrap {
        command.args(["--bootstrap", address]);
    }
    if produce {
        command.args(["--produce-block-interval-ms", "1000"]);
    }
    let mut child = command.spawn().context("starting node")?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let stderr = child.stderr.take().expect("stderr is piped");
    nodes.children.push(child);
    let log = Arc::new(Mutex::new(log));
    for stream in [
        Box::new(stdout) as Box<dyn std::io::Read + Send>,
        Box::new(stderr),
    ] {
        let name = name.to_owned();
        let log = Arc::clone(&log);
        nodes.log_threads.push(thread::spawn(move || {
            for line in BufReader::new(stream).lines() {
                let line = line?;
                writeln!(log.lock().expect("log mutex poisoned"), "{line}")?;
                // Lock for a complete line so different nodes cannot interleave it.
                writeln!(std::io::stdout().lock(), "[{name}] {line}")?;
            }
            Ok(())
        }));
    }
    Ok(())
}

fn log_field(log: &str, message: &str, field: &str) -> Option<String> {
    log.lines()
        .find(|line| line.contains(message))?
        .split_whitespace()
        .find_map(|word| word.strip_prefix(field).map(str::to_owned))
}

async fn wait_for_tip(path: &Path, minimum: u64, nodes: &mut Nodes) -> anyhow::Result<Block> {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        nodes.check_alive()?;
        if let Some(tip) = ChainStore::open(path)?.tip().await
            && tip.height >= minimum
        {
            return Ok(tip);
        }
        ensure!(
            Instant::now() < deadline,
            "{} did not reach height {minimum} within 45s",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore = "starts six real TCP nodes; run scripts/test-late-join-sync.sh"]
async fn late_joiner_downloads_history_and_follows_new_blocks() -> anyhow::Result<()> {
    // Keep logs and chains for diagnosing either a pass or a failure.
    let directory: PathBuf = tempfile::tempdir()?.keep();
    println!("Test logs and chains: {}", directory.display());
    let mut nodes = Nodes::default();
    let validators: Vec<_> = (1..=5)
        .map(|i| validator_id_from_seed(&format!("node-{i}")))
        .collect();
    fs::write(
        directory.join("network.toml"),
        format!("[genesis]\nchain_id = 'late-join-test'\nvalidators = {validators:?}\n"),
    )?;
    let genesis_store = ChainStore::new();
    SimpleConsensus::with_genesis(
        "independent-observer",
        genesis_store.clone(),
        "late-join-test",
        validators,
    )
    .await?;
    let genesis = genesis_store
        .tip()
        .await
        .context("missing configured genesis")?;
    ensure!(
        genesis.height == 0 && genesis.parent_hash.is_none(),
        "invalid genesis"
    );
    start_node(&mut nodes, &directory, "node-1", None, true)?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let bootstrap = loop {
        nodes.check_alive()?;
        let log = fs::read_to_string(directory.join("node-1.log"))?;
        if let (Some(address), Some(peer)) = (
            log_field(&log, "network listening", "address="),
            log_field(&log, "local peer id", "local_peer_id="),
        ) {
            break format!("{address}/p2p/{peer}");
        }
        ensure!(
            Instant::now() < deadline,
            "bootstrap node did not start listening"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    for i in 2..=5 {
        start_node(
            &mut nodes,
            &directory,
            &format!("node-{i}"),
            Some(&bootstrap),
            true,
        )?;
    }
    println!("Five validators started; waiting 15 seconds before joining...");
    for _ in 0..15 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        nodes.check_alive()?;
    }
    for i in 1..=5 {
        let store = ChainStore::open(directory.join(format!("node-{i}.bin")))?;
        ensure!(
            store.get(&genesis.hash).await == Some(genesis.clone()),
            "node-{i} did not initialize the shared genesis"
        );
    }
    println!("All five validators share genesis {}", genesis.hash);
    let source_path = directory.join("node-1.bin");
    let before = ChainStore::open(&source_path)?
        .tip()
        .await
        .context("network produced no blocks")?;
    ensure!(
        before.height >= 5,
        "network did not complete a validator rotation"
    );
    let late_path = directory.join("late-observer.bin");
    ensure!(
        !late_path.exists(),
        "late joiner must start without a chain"
    );
    println!(
        "Starting a fresh node from shared genesis; network height is {}",
        before.height
    );
    // An observer is outside the fixed validator schedule: adding a sixth producer
    // would change consensus membership rather than test synchronization alone.
    start_node(
        &mut nodes,
        &directory,
        "late-observer",
        Some(&bootstrap),
        false,
    )?;
    wait_for_tip(&late_path, 0, &mut nodes).await?;
    ensure!(
        ChainStore::open(&late_path)?.get(&genesis.hash).await == Some(genesis.clone()),
        "late observer did not initialize the shared genesis"
    );
    println!(
        "Late observer has shared block 0; waiting for history through height {}",
        before.height
    );
    wait_for_tip(&late_path, before.height, &mut nodes).await?;
    let late = ChainStore::open(&late_path)?;
    let source = ChainStore::open(&source_path)?;
    let mut block = before.clone();
    loop {
        ensure!(
            late.get(&block.hash).await.as_ref() == Some(&block),
            "late joiner missing historical block at height {}",
            block.height
        );
        match block.parent_hash {
            Some(parent) => {
                block = source
                    .get(&parent)
                    .await
                    .context("source missing ancestor")?
            }
            None => {
                ensure!(block.height == 0, "history did not reach genesis");
                break;
            }
        }
    }
    println!(
        "Historical sync verified through height {} (including genesis)",
        before.height
    );
    let caught_up = ChainStore::open(&late_path)?
        .tip()
        .await
        .context("missing synced tip")?;
    let future = wait_for_tip(&source_path, caught_up.height + 5, &mut nodes).await?;
    wait_for_tip(&late_path, future.height, &mut nodes).await?;
    for name in [
        "node-1",
        "node-2",
        "node-3",
        "node-4",
        "node-5",
        "late-observer",
    ] {
        let path = directory.join(format!("{name}.bin"));
        wait_for_tip(&path, future.height, &mut nodes).await?;
        ensure!(
            ChainStore::open(path)?.get(&future.hash).await == Some(future.clone()),
            "{name} disagrees at height {}",
            future.height
        );
        let log = fs::read_to_string(directory.join(format!("{name}.log")))?;
        ensure!(
            !log.contains("peer banned")
                && !log.contains("rejecting banned peer")
                && !log.contains("ingress rate limit was exceeded"),
            "{name} banned a peer; inspect logs"
        );
    }
    println!(
        "PASS: all six nodes agree at height {}; late joiner follows new blocks",
        future.height
    );
    Ok(())
}
