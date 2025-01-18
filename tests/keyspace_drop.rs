#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
fn whitebox_keyspace_drop() -> fjall::Result<()> {
    use fjall::Config;

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(4, fjall::drop::load_drop_counter());

        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(4, fjall::drop::load_drop_counter());

        let partition = keyspace.open_partition("default", Default::default())?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        drop(partition);
        drop(keyspace);
        assert_eq!(0, fjall::drop::load_drop_counter());
    }

    {
        let folder = tempfile::tempdir()?;

        assert_eq!(0, fjall::drop::load_drop_counter());
        let keyspace = Config::new(folder).open()?;
        assert_eq!(4, fjall::drop::load_drop_counter());

        let _partition = keyspace.open_partition("default", Default::default())?;
        assert_eq!(5, fjall::drop::load_drop_counter());

        let _partition2 = keyspace.open_partition("different", Default::default())?;
        assert_eq!(6, fjall::drop::load_drop_counter());
    }

    assert_eq!(0, fjall::drop::load_drop_counter());

    Ok(())
}

#[cfg(feature = "__internal_whitebox")]
#[test_log::test]
fn whitebox_keyspace_drop_2() -> fjall::Result<()> {
    use fjall::{Config, PartitionCreateOptions};

    let folder = tempfile::tempdir()?;

    {
        let keyspace = Config::new(&folder).open()?;

        let partition = keyspace.open_partition("tree", PartitionCreateOptions::default())?;
        let partition2 = keyspace.open_partition("tree1", PartitionCreateOptions::default())?;

        partition.insert("a", "a")?;
        partition2.insert("b", "b")?;

        partition.rotate_memtable_and_wait()?;
    }

    assert_eq!(0, fjall::drop::load_drop_counter());

    Ok(())
}
