use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use ignore::WalkBuilder;
use sha2::{Digest, Sha256};
use std::{fs, path::PathBuf};

/// Shard a flat folder of files into a sharded directory structure.
/// Only move files matching file-type. Respects .gitignore.
#[derive(Parser, Debug)]
#[command(name = "sharder")]
#[command(author, version, about)]
struct Args {
    /// Source folder with files
    #[arg(short, long)]
    source: PathBuf,

    /// Output folder where sharded files will be stored
    #[arg(short, long)]
    target: PathBuf,

    /// Sharding strategy (by filename or file content hash)
    #[arg(short, long, value_enum, default_value = "filename")]
    mode: ShardMode,

    /// Shard files with matching file extension
    #[arg(short, long, default_value = "md")]
    file_type: String,

    /// Number of characters used for sharding
    #[arg(long, default_value_t = 2)]
    shard_len: usize,

    /// Depth of folders to recursively walk. By default walk all sub trees.
    #[arg(long)]
    depth: Option<usize>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ShardMode {
    Filename,
    Content,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let walker = WalkBuilder::new(&args.source)
        .standard_filters(true)
        .max_depth(args.depth)
        .build();

    for result in walker {
        let entry = match result {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Error: {e}");
                continue;
            }
        };

        let path = entry.path();

        if path
            .extension()
            .map(|ext| *ext != *args.file_type)
            .unwrap_or(true)
        {
            continue; // skip non-matching files
        }

        let file_name = path
            .file_name()
            .context("error getting filename")?
            .to_string_lossy()
            .to_string();
        let content = fs::read(path).with_context(|| format!("error reading `{path:?}`"))?;

        let full_key = match args.mode {
            ShardMode::Filename => &file_name,
            ShardMode::Content => &hex_sha256(&content),
        };

        let shard_prefix = &full_key[..args.shard_len.min(full_key.len())].to_lowercase();

        let output_dir = args.target.join(shard_prefix);
        fs::create_dir_all(&output_dir)?;

        let target_path = output_dir.join(&file_name);
        fs::write(&target_path, &content)?;
        println!("Wrote {}", target_path.display());
    }

    Ok(())
}

fn hex_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}
