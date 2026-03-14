use super::*;

impl LocalThroughputState {
    pub fn coordinator_paths(
        db_root: impl AsRef<Path>,
        checkpoint_root: impl AsRef<Path>,
    ) -> LocalThroughputCoordinatorPaths {
        LocalThroughputCoordinatorPaths {
            db_path: db_root.as_ref().join("coordinator"),
            checkpoint_dir: checkpoint_root.as_ref().join("coordinator"),
        }
    }

    pub fn open_coordinator(
        db_root: impl AsRef<Path>,
        checkpoint_root: impl AsRef<Path>,
        checkpoint_retention: usize,
    ) -> Result<Self> {
        let paths = Self::coordinator_paths(db_root, checkpoint_root);
        Self::open(paths.db_path, paths.checkpoint_dir, checkpoint_retention)
    }

    pub fn partition_shard_paths(
        db_root: impl AsRef<Path>,
        checkpoint_root: impl AsRef<Path>,
        throughput_partition_id: i32,
    ) -> LocalThroughputShardPaths {
        let shard_dir = format!("partition-{throughput_partition_id}");
        LocalThroughputShardPaths {
            throughput_partition_id,
            db_path: db_root.as_ref().join("shards").join(&shard_dir),
            checkpoint_dir: checkpoint_root.as_ref().join("shards").join(shard_dir),
        }
    }

    pub fn open_partition_shard(
        db_root: impl AsRef<Path>,
        checkpoint_root: impl AsRef<Path>,
        throughput_partition_id: i32,
        checkpoint_retention: usize,
    ) -> Result<Self> {
        let paths = Self::partition_shard_paths(db_root, checkpoint_root, throughput_partition_id);
        Self::open(paths.db_path, paths.checkpoint_dir, checkpoint_retention)
    }

    pub fn open(
        db_path: impl Into<PathBuf>,
        checkpoint_dir: impl Into<PathBuf>,
        checkpoint_retention: usize,
    ) -> Result<Self> {
        let db_path = db_path.into();
        let checkpoint_dir = checkpoint_dir.into();
        fs::create_dir_all(&db_path).with_context(|| {
            format!("failed to create throughput state dir {}", db_path.display())
        })?;
        fs::create_dir_all(&checkpoint_dir).with_context(|| {
            format!("failed to create throughput checkpoint dir {}", checkpoint_dir.display())
        })?;
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(
            &options,
            &db_path,
            vec![
                ColumnFamilyDescriptor::new(META_CF, meta_cf_options()),
                ColumnFamilyDescriptor::new(BATCHES_CF, batches_cf_options()),
                ColumnFamilyDescriptor::new(CHUNKS_CF, chunks_cf_options()),
                ColumnFamilyDescriptor::new(GROUPS_CF, groups_cf_options()),
                ColumnFamilyDescriptor::new(BATCH_INDEXES_CF, batch_indexes_cf_options()),
                ColumnFamilyDescriptor::new(SCHEDULING_CF, scheduling_cf_options()),
                ColumnFamilyDescriptor::new(STREAM_JOBS_CF, batches_cf_options()),
            ],
        )
        .with_context(|| format!("failed to open throughput state db {}", db_path.display()))?;
        let state = Self {
            db: Arc::new(db),
            db_path,
            checkpoint_dir,
            checkpoint_retention: checkpoint_retention.max(1),
            meta: Arc::new(Mutex::new(LocalStateMeta::default())),
            lease_lock: Arc::new(Mutex::new(())),
        };
        state.migrate_legacy_default_cf()?;
        Ok(state)
    }

    pub(super) fn cf(&self, name: &'static str) -> &ColumnFamily {
        self.db
            .cf_handle(name)
            .unwrap_or_else(|| panic!("missing throughput RocksDB column family {name}"))
    }

    fn migrate_legacy_default_cf(&self) -> Result<()> {
        let mut migrated_entries = 0_u64;
        let mut write_batch = WriteBatch::default();

        for entry in self.db.iterator(IteratorMode::Start) {
            let (key, value) = entry
                .context("failed to iterate legacy throughput state in default column family")?;
            let key = String::from_utf8(key.to_vec())
                .context("legacy throughput state key is not utf-8")?;
            let target_cf = legacy_cf_for_key(&key).ok_or_else(|| {
                anyhow::anyhow!("unexpected legacy throughput key in default column family: {key}")
            })?;
            if self
                .db
                .get_cf(self.cf(target_cf), &key)
                .with_context(|| format!("failed to probe migrated legacy throughput key {key}"))?
                .is_none()
            {
                write_batch.put_cf(self.cf(target_cf), &key, value.to_vec());
            }
            write_batch.delete(&key);
            migrated_entries = migrated_entries.saturating_add(1);
        }

        if migrated_entries == 0 {
            return Ok(());
        }

        self.db
            .write(write_batch)
            .context("failed to migrate legacy throughput default column family state")?;
        let mut meta = self.meta.lock().expect("local throughput meta lock poisoned");
        meta.legacy_default_cf_entries_migrated =
            meta.legacy_default_cf_entries_migrated.saturating_add(migrated_entries);
        meta.last_legacy_default_cf_migration_at = Some(Utc::now());
        Ok(())
    }

    pub(super) fn get_cf_with_legacy_fallback(
        &self,
        cf_name: &'static str,
        key: &str,
        subject: &str,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self
            .db
            .get_cf(self.cf(cf_name), key)
            .with_context(|| format!("failed to load {subject} from column family {cf_name}"))?
        {
            return Ok(Some(value));
        }
        self.db
            .get(key)
            .with_context(|| format!("failed to load legacy {subject} from default column family"))
    }

    pub(super) fn write_batch_put_cf(
        &self,
        write_batch: &mut WriteBatch,
        cf_name: &'static str,
        key: &str,
        value: Vec<u8>,
    ) {
        write_batch.put_cf(self.cf(cf_name), key, value);
    }

    pub(super) fn write_batch_delete_cf_and_legacy(
        &self,
        write_batch: &mut WriteBatch,
        cf_name: &'static str,
        key: &str,
    ) {
        write_batch.delete_cf(self.cf(cf_name), key);
        write_batch.delete(key);
    }

    pub(super) fn load_prefixed_entries(
        &self,
        cf_name: &'static str,
        prefix: &str,
        subject: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let mut entries = Vec::new();
        for entry in self.db.iterator_cf(
            self.cf(cf_name),
            IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        ) {
            let (key, value) = entry.with_context(|| {
                format!("failed to iterate {subject} in column family {cf_name}")
            })?;
            let key = String::from_utf8(key.to_vec())
                .with_context(|| format!("{subject} key is not utf-8"))?;
            if !key.starts_with(prefix) {
                break;
            }
            entries.push((key, value.to_vec()));
        }
        Ok(entries)
    }
}
