use {
    alpamayo::config::Config,
    clap::Parser,
    rocksdb::{DB, Options},
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Alpamayo: RocksDB storage inspector")]
struct Args {
    #[clap(short, long, default_value_t = String::from("config.yml"))]
    pub config: String,
}

const CF_NAMES: &[&str] = &[
    "slot_basic_index",
    "slot_extra_index",
    "tx_index",
    "sfa_index",
    "ir_index",
];

fn human(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < units.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.2} {}", units[unit])
}

fn parse_agg_prop(props: &str, key: &str) -> u64 {
    props
        .split(';')
        .find_map(|s| {
            s.trim()
                .strip_prefix(key)
                .and_then(|v| v.trim_start_matches('=').trim().parse().ok())
        })
        .unwrap_or(0)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)?;
    let primary_path = &config.storage.rocksdb.path;

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(false);
    // avoid needing a huge ulimit -n just to read CF properties
    opts.set_max_open_files(512);

    let db = DB::open_cf_for_read_only(&opts, primary_path, CF_NAMES, false)?;

    let mut total_sst: u64 = 0;
    let mut total_live: u64 = 0;
    let mut total_tombstones: u64 = 0;
    let mut total_pending: u64 = 0;

    println!(
        "{:<20} {:>14} {:>14} {:>16} {:>16} {:>16} {:>16}",
        "CF", "num_keys", "tombstones", "sst_size", "live_data", "waste", "pending_compact"
    );
    for name in CF_NAMES {
        let cf = db.cf_handle(name).expect("cf must exist");

        let num_keys: u64 = db
            .property_int_value_cf(cf, "rocksdb.estimate-num-keys")?
            .unwrap_or(0);
        let sst_size: u64 = db
            .property_int_value_cf(cf, "rocksdb.total-sst-files-size")?
            .unwrap_or(0);
        let live_size: u64 = db
            .property_int_value_cf(cf, "rocksdb.estimate-live-data-size")?
            .unwrap_or(0);
        let pending_compact: u64 = db
            .property_int_value_cf(cf, "rocksdb.estimate-pending-compaction-bytes")?
            .unwrap_or(0);

        let props = db
            .property_value_cf(cf, "rocksdb.aggregated-table-properties")?
            .unwrap_or_default();
        let tombstones = parse_agg_prop(&props, "# deletions");

        let waste = sst_size.saturating_sub(live_size);

        total_sst += sst_size;
        total_live += live_size;
        total_tombstones += tombstones;
        total_pending += pending_compact;

        println!(
            "{:<20} {:>14} {:>14} {:>16} {:>16} {:>16} {:>16}",
            name,
            num_keys,
            tombstones,
            human(sst_size),
            human(live_size),
            human(waste),
            human(pending_compact),
        );
    }

    let total_waste = total_sst.saturating_sub(total_live);
    let space_amp = if total_live > 0 {
        total_sst as f64 / total_live as f64
    } else {
        0.0
    };

    println!("---");
    println!("total sst size:       {}", human(total_sst));
    println!("total live data:      {}", human(total_live));
    println!("total waste:          {}", human(total_waste));
    println!("total tombstones:     {total_tombstones}");
    println!("total pending compact:{}", human(total_pending));
    println!("space amplification:  {space_amp:.2}x");

    Ok(())
}
