use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use memds_proto::memds_api::{CountRes, OpResult, OpType, TimeRes};
use memds_proto::Atom;

fn systime() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

pub fn dbsize(db: &mut HashMap<Vec<u8>, Atom>) -> OpResult {
    let mut info_res = CountRes::new();
    info_res.n = db.len() as u64;

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SRV_DBSIZE;
    op_res.set_count(info_res);

    op_res
}

pub fn flush(db: &mut HashMap<Vec<u8>, Atom>, otype: OpType) -> OpResult {
    db.clear();

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = otype;

    op_res
}

pub fn time() -> OpResult {
    let now = systime();

    let mut time_res = TimeRes::new();
    time_res.secs = now.as_secs();
    time_res.nanosecs = now.subsec_nanos();

    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = OpType::SRV_TIME;
    op_res.set_srv_time(time_res);

    op_res
}

#[cfg(test)]
mod tests {
    use crate::server;
    use memds_proto::memds_api::OpType;
    use memds_proto::Atom;
    use std::collections::HashMap;

    fn get_test_db() -> HashMap<Vec<u8>, Atom> {
        let mut db: HashMap<Vec<u8>, Atom> = HashMap::new();
        db.insert(b"foo".to_vec(), Atom::String(b"bar".to_vec()));
        db.insert(b"name".to_vec(), Atom::String(b"Jane Doe".to_vec()));
        db.insert(b"age".to_vec(), Atom::String(b"25".to_vec()));

        db
    }

    #[test]
    fn dbsize() {
        let mut db = get_test_db();

        let res = server::dbsize(&mut db);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SRV_DBSIZE);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 3);
    }

    #[test]
    fn flush() {
        let mut db = get_test_db();

        let res = server::flush(&mut db, OpType::SRV_FLUSHALL);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SRV_FLUSHALL);

        let res = server::dbsize(&mut db);
        let count_res = res.get_count();
        assert_eq!(count_res.n, 0);

        let res = server::flush(&mut db, OpType::SRV_FLUSHDB);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SRV_FLUSHDB);

        let res = server::dbsize(&mut db);
        let count_res = res.get_count();
        assert_eq!(count_res.n, 0);
    }
}
