use std::collections::HashMap;

use memds_proto::util::result_err;
use memds_proto::{OpResult, OpType, StrGetOp, StrGetRes, StrSetOp, StrSetRes};

pub fn get(db: &mut HashMap<Vec<u8>, Vec<u8>>, req: &StrGetOp) -> OpResult {
    match db.get(req.get_key()) {
        Some(value) => {
            let mut get_res = StrGetRes::new();
            if req.want_length {
                get_res.set_value_length(value.len() as u64);
            } else {
                get_res.set_value(value.to_vec());
            }

            let mut op_res = OpResult::new();
            op_res.ok = true;
            op_res.otype = OpType::STR_GET;
            op_res.set_get(get_res);

            op_res
        }
        None => result_err(-404, "Not Found"),
    }
}

pub fn set(db: &mut HashMap<Vec<u8>, Vec<u8>>, req: &StrSetOp) -> OpResult {
    let previous = db.insert(req.get_key().to_vec(), req.get_value().to_vec());

    let mut set_res = StrSetRes::new();
    if req.return_old && previous.is_some() {
        set_res.set_old_value(previous.unwrap());
    }

    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = OpType::STR_SET;
    op_res.set_set(set_res);

    op_res
}
