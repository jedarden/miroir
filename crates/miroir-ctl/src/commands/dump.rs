use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum DumpSubcommand {
    /// Import a Meilisearch dump file into Miroir
    ///
    /// Imports use streaming mode by default, routing documents via the public API.
    /// Falls back to broadcast mode for incompatible dump variants.
    ///
    /// See compatibility matrix: docs/dump-import/compatibility-matrix.md
    Import {
        /// Path to the .dump file
        #[arg(short, long)]
        file: String,

        /// Target index UID (required for single-index dumps)
        #[arg(short, long)]
        index: Option<String>,

        /// Import mode: 'streaming' (default) or 'broadcast' (legacy)
        ///
        /// Streaming routes documents per-shard for optimal storage distribution.
        /// Broadcast sends all documents to all nodes, requiring post-import rebalance.
        #[arg(short, long, default_value = "streaming")]
        mode: String,

        /// Batch size for document streaming (documents per POST per target node)
        #[arg(long, default_value = "1000")]
        batch_size: usize,

        /// Maximum concurrent in-flight POSTs across target nodes
        #[arg(long, default_value = "8")]
        parallel_writes: usize,
    },

    /// Export data from Miroir to a dump file
    ///
    /// Creates a Meilisearch-compatible dump by fan-out collection and merge.
    Export {
        /// Output file path (.dump extension recommended)
        #[arg(short, long)]
        output: String,

        /// Index UID to export (omit for all indexes)
        #[arg(short, long)]
        index: Option<String>,

        /// Include task history in dump
        #[arg(long, default_value = "false")]
        include_tasks: bool,
    },

    /// Analyze a dump file for compatibility with streaming import mode
    ///
    /// Scans the dump and reports whether streaming mode can fully reconstruct it,
    /// or if broadcast fallback is required. References the compatibility matrix.
    Analyze {
        /// Path to the .dump file to analyze
        #[arg(short, long)]
        file: String,
    },
}

pub async fn run(cmd: DumpSubcommand) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        DumpSubcommand::Import { file, index, mode, .. } => {
            Err(format!(
                "Dump import is not yet implemented. See bead miroir-zc2.5 for tracking.\n\n\
                 For dump compatibility information, see:\n\
                 docs/dump-import/compatibility-matrix.md\n\n\
                 Requested:\n\
                 File: {file}\n\
                 Index: {index}\n\
                 Mode: {mode}"
            ).into())
        }
        DumpSubcommand::Export { output, index, .. } => {
            Err(format!(
                "Dump export is not yet implemented. See bead miroir-qon for tracking.\n\n\
                 Requested:\n\
                 Output: {output}\n\
                 Index: {index:?}"
            ).into())
        }
        DumpSubcommand::Analyze { file } => {
            Err(format!(
                "Dump analysis is not yet implemented. See bead miroir-zc2.5 for tracking.\n\n\
                 This command will analyze {file} and report:\n\
                 - Whether streaming mode can reconstruct the dump\n\
                 - Any field conflicts (e.g., existing `_miroir_shard`)\n\
                 - Meilisearch version compatibility\n\
                 - Recommended import mode\n\n\
                 See compatibility matrix:\n\
                 docs/dump-import/compatibility-matrix.md"
            ).into())
        }
    }
}
