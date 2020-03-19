use bytes::BytesMut;
use nix::unistd::{fork, ForkResult};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, SystemTime};
use tokio_util::codec::Encoder;

use crate::keys;
use memds_proto::memds_api::{
    CountRes, MemdsMessage, MemdsMessage_MsgType, OpResult, OpType, TimeRes,
};
use memds_proto::util::result_err;
use memds_proto::{Atom, MemdsCodec};

const EXPORT_FN: &'static str = "memds-export.dat";

fn systime() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
}

pub fn dbsize(db: &mut HashMap<Vec<u8>, Atom>) -> OpResult {
    // query db item count
    let mut info_res = CountRes::new();
    info_res.n = db.len() as u64;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SRV_DBSIZE;
    op_res.set_count(info_res);

    op_res
}

pub fn flush(db: &mut HashMap<Vec<u8>, Atom>, otype: OpType) -> OpResult {
    // clear entire db; we only have 1 db right now,
    // making flush [one] db operation equivalent to flush-all-dbs.
    db.clear();

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = otype;

    op_res
}

pub fn time() -> OpResult {
    // query system time
    let now = systime();

    // calculate secs & nanosecs
    let mut time_res = TimeRes::new();
    time_res.secs = now.as_secs();
    time_res.nanosecs = now.subsec_nanos();

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = OpType::SRV_TIME;
    op_res.set_srv_time(time_res);

    op_res
}

pub fn bgsave(db: &mut HashMap<Vec<u8>, Atom>) -> OpResult {
    match fork() {
        Ok(ForkResult::Parent { child: _, .. }) => {
            // standard operation result assignment & final return
            let mut op_res = OpResult::new();

            op_res.ok = true;
            op_res.otype = OpType::SRV_BGSAVE;

            return op_res;
        }
        Ok(ForkResult::Child) => {}

        Err(_) => {
            return result_err(-500, "Internal error - fork");
        }
    }

    // child continues...

    let f_res = File::create(EXPORT_FN);
    if f_res.is_err() {
        println!("Internal error I/O - create");
        std::process::exit(1);
    }
    let mut f = f_res.unwrap();
    let mut codec = MemdsCodec::new();

    for key in db.keys() {
        // serialize key+value into protobuf message
        let dbv = keys::element_dbv(db, key).unwrap();
        let mut msg = MemdsMessage::new();
        msg.mtype = MemdsMessage_MsgType::DBVAL;
        msg.set_dbv(dbv);

        // encode message into checksummed stream
        let msg_raw = &mut BytesMut::new();
        codec.encode(msg, msg_raw).unwrap();

        // write packet to file
        let res = f.write_all(msg_raw);
        if res.is_err() {
            println!("Internal error I/O - write");
            std::process::exit(1);
        }
    }

    // stream terminator
    let mut end_msg = MemdsMessage::new();
    end_msg.mtype = MemdsMessage_MsgType::END;

    // encode terminator message into checksummed stream
    let end_msg_raw = &mut BytesMut::new();
    codec.encode(end_msg, end_msg_raw).unwrap();

    // write terminating packet to file
    let res = f.write_all(end_msg_raw);
    if res.is_err() {
        println!("Internal error I/O - write end");
        std::process::exit(1);
    }

    // flush to disk
    if f.sync_data().is_err() {
        println!("Internal error I/O - sync");
        std::process::exit(1);
    }

    std::process::exit(0);
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
