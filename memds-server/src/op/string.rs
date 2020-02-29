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

#[cfg(test)]
mod tests {
    use crate::op;
    use memds_proto::{OpType, StrGetOp, StrSetOp};
    use std::collections::HashMap;

    fn get_test_db() -> HashMap<Vec<u8>, Vec<u8>> {
        let mut db: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        db.insert(b"foo".to_vec(), b"bar".to_vec());
        db.insert(b"name".to_vec(), b"Jane Doe".to_vec());
        db.insert(b"age".to_vec(), b"25".to_vec());

        db
    }

    #[test]
    fn basic_get() {
        let mut db = get_test_db();

        let mut req = StrGetOp::new();
        req.set_key(b"foo".to_vec());
        req.set_want_length(false);

        let res = op::string::get(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_GET);

        let get_res = res.get_get();
        assert_eq!(get_res.value, b"bar".to_vec());
    }

    #[test]
    fn get_length() {
        let mut db = get_test_db();

        let mut req = StrGetOp::new();
        req.set_key(b"foo".to_vec());
        req.set_want_length(true);

        let res = op::string::get(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_GET);

        let get_res = res.get_get();
        assert_eq!(get_res.value_length, 3);
    }

    #[test]
    fn get_not_found() {
        let mut db = get_test_db();

        let mut req = StrGetOp::new();
        req.set_key(b"does not exist".to_vec());
        req.set_want_length(false);

        let res = op::string::get(&mut db, &req);

        assert_eq!(res.ok, false);
        assert_eq!(res.otype, OpType::NOOP);
        assert_eq!(res.err_code, -404);
    }

    #[test]
    fn basic_set() {
        let mut db = get_test_db();

        let mut req = StrSetOp::new();
        req.set_key(b"barn".to_vec());
        req.set_value(b"door".to_vec());
        req.set_return_old(true);

        let res = op::string::set(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_SET);

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"".to_vec());
    }
}
