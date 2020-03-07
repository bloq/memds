use std::time::{Duration, SystemTime};

use memds_proto::memds_api::{OpResult, OpType, TimeRes};

fn systime() -> Duration {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
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
