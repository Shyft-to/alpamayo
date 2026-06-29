use rocksdb::{DB, Options};
use std::env;

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

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let primary_path = args.get(1).expect("arg1: primary rocksdb path");

    let mut opts = Options::default();
    opts.create_if_missing(false);
    opts.create_missing_column_families(false);
    // avoid needing a huge ulimit -n just to read CF properties
    opts.set_max_open_files(512);

    let db = DB::open_cf_for_read_only(&opts, primary_path, CF_NAMES, false)?;

    let mut total: u64 = 0;
    println!(
        "{:<20} {:>14} {:>16} {:>16}",
        "CF", "num_keys", "sst_size", "live_data"
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

        total += sst_size;
        println!(
            "{:<20} {:>14} {:>16} {:>16}",
            name,
            num_keys,
            human(sst_size),
            human(live_size)
        );
    }
    println!("---");
    println!("total sst size: {}", human(total));

    Ok(())
}
