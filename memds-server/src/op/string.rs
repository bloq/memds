use std::collections::HashMap;
use std::str;

use memds_proto::memds_api::{
    NumOp, NumRes, OpResult, OpType, StrGetOp, StrGetRes, StrSetOp, StrSetRes,
};
use memds_proto::util::result_err;

pub fn incrdecr(db: &mut HashMap<Vec<u8>, Vec<u8>>, otype: OpType, req: &NumOp) -> OpResult {
    // parameterize based on operation
    let (has_n, is_incr) = match otype {
        OpType::DECR => (false, false),
        OpType::DECRBY => (true, false),
        OpType::INCR => (false, true),
        OpType::INCRBY => (true, true),
        _ => unreachable!(),
    };

    // get old value from db, or init
    let old_val: i64 = match db.get(req.get_key()) {
        None => 0,
        Some(val) => {
            let s = {
                let s_res = str::from_utf8(val);
                if s_res.is_err() {
                    return result_err(-400, "value not a string");
                }
                s_res.unwrap()
            };

            let sv_res = s.parse::<i64>();
            if sv_res.is_err() {
                return result_err(-400, "value not i64");
            }

            sv_res.unwrap()
        }
    };

    // determine inc/dec operand
    let n = match has_n {
        true => req.n,
        false => 1,
    };

    // perform inc/dec
    let new_val = match is_incr {
        true => old_val + n,
        false => old_val - n,
    };

    // store value in database as string
    db.insert(
        req.get_key().to_vec(),
        new_val.to_string().as_bytes().to_vec(),
    );

    // return success(old value)
    let mut num_res = NumRes::new();
    num_res.old_value = old_val;

    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = otype;
    op_res.set_num(num_res);

    op_res
}

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
    use memds_proto::memds_api::{NumOp, OpType, StrGetOp, StrSetOp};
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
        assert!(res.has_get());
        assert!(!res.has_set());

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
        assert!(res.has_get());
        assert!(!res.has_set());

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
        assert!(!res.has_get());
        assert!(!res.has_set());
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
        assert!(!res.has_get());
        assert!(res.has_set());

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"".to_vec());
    }

    #[test]
    fn set_with_old_value() {
        let mut db = get_test_db();

        let mut req = StrSetOp::new();
        req.set_key(b"foo".to_vec());
        req.set_value(b"door".to_vec());
        req.set_return_old(true);

        let res = op::string::set(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_SET);
        assert!(!res.has_get());
        assert!(res.has_set());

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"bar".to_vec()); // expect: old value

        let mut req = StrSetOp::new();
        req.set_key(b"foo".to_vec());
        req.set_value(b"door".to_vec());

        let res = op::string::set(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_SET);
        assert!(!res.has_get());
        assert!(res.has_set());

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"".to_vec()); // expect: blank
    }

    #[test]
    fn basic_incr_decr() {
        let mut db = get_test_db();

        let mut req = NumOp::new();
        req.set_key(b"num".to_vec());
        req.n = 0;

        // INCR(item not yet in db) => 1; old-value==0
        let res = op::string::incrdecr(&mut db, OpType::INCR, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::INCR);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"0");

        // DECR(num) => 0; old-value==1
        let res = op::string::incrdecr(&mut db, OpType::DECR, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::DECR);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"1");

        // DECRBY(num,2) => -2; old-value==0
        req.n = 2;
        let res = op::string::incrdecr(&mut db, OpType::DECRBY, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::DECRBY);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"0");

        // INCRBY(num,2) => 0; old-value==-2
        let res = op::string::incrdecr(&mut db, OpType::INCRBY, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::INCRBY);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"-2");

        // verify final value is indeed 0, from previous operation
        let mut req = StrGetOp::new();
        req.set_key(b"num".to_vec());

        let res = op::string::get(&mut db, &req);
        assert_eq!(res.ok, true);
        let get_res = res.get_get();
        assert_eq!(get_res.value, b"0".to_vec());
    }
}
