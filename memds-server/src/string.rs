use std::collections::HashMap;
use std::str;

use memds_proto::memds_api::{
    NumOp, NumRes, OpResult, OpType, StrGetOp, StrGetRes, StrSetOp, StrSetRes,
};
use memds_proto::util::result_err;
use memds_proto::Atom;

pub fn incrdecr(db: &mut HashMap<Vec<u8>, Atom>, otype: OpType, req: &NumOp) -> OpResult {
    // parameterize based on operation
    let (has_n, is_incr) = match otype {
        OpType::STR_DECR => (false, false),
        OpType::STR_DECRBY => (true, false),
        OpType::STR_INCR => (false, true),
        OpType::STR_INCRBY => (true, true),
        _ => unreachable!(),
    };

    // get old value from db, or init
    let old_val: i64 = match db.get(req.get_key()) {
        None => 0,
        Some(atom) => {
            let s = {
                match atom {
                    Atom::String(val) => {
                        let s_res = str::from_utf8(val);
                        if s_res.is_err() {
                            return result_err(-400, "value not a string");
                        }
                        s_res.unwrap()
                    }
                    _ => {
                        return result_err(-400, "value not a string");
                    }
                }
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
        Atom::String(new_val.to_string().as_bytes().to_vec()),
    );

    // return success(old value)
    let mut num_res = NumRes::new();
    num_res.old_value = old_val;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = otype;
    op_res.set_num(num_res);

    op_res
}

fn str_index(len: i64, pos_requested: i64) -> usize {
    // non-negative positions are absolute.
    // negative positions relative to end of buffer
    let mut pos = {
        if pos_requested >= 0 {
            pos_requested
        } else {
            len + pos_requested + 1
        }
    };

    // clamp values to unsigned buffer bounds
    if pos < 0 {
        pos = 0;
    } else if pos > len {
        pos = len;
    }

    pos as usize
}

fn sanitize_range(in_start: i32, in_end: i32, value_len: i32) -> (usize, usize) {
    let start = str_index(value_len as i64, in_start as i64);
    let mut end = str_index(value_len as i64, in_end as i64);
    if start > end {
        end = start;
    }

    (start, end)
}

pub fn get(db: &mut HashMap<Vec<u8>, Atom>, req: &StrGetOp, otype: OpType) -> OpResult {
    // get item by key
    match db.get(req.get_key()) {
        Some(atom) => match atom {
            Atom::String(value) => {
                let mut get_res = StrGetRes::new();
                if req.want_length {
                    get_res.set_value_length(value.len() as u64);
                } else if otype == OpType::STR_GETRANGE {
                    let (start, end) =
                        sanitize_range(req.range_start, req.range_end, value.len() as i32);
                    let sub = &value[start..end];
                    get_res.set_value(sub.to_vec());
                } else {
                    get_res.set_value(value.to_vec());
                }

                // standard operation result assignment & final return
                let mut op_res = OpResult::new();
                op_res.ok = true;
                op_res.otype = otype;
                op_res.set_get(get_res);

                op_res
            }
            _ => result_err(-400, "not a string"),
        },
        None => result_err(-404, "Not Found"),
    }
}

pub fn set(db: &mut HashMap<Vec<u8>, Atom>, req: &StrSetOp) -> OpResult {
    let key = req.get_key();

    // option test: create iff key does not exist
    if req.create_excl && db.contains_key(key) {
        return result_err(-412, "Precondition failed: key exists");
    }

    // insert, and return previous item stored at key (if any)
    let previous = db.insert(key.to_vec(), Atom::String(req.get_value().to_vec()));

    // if old-value requested, return it
    let mut set_res = StrSetRes::new();
    if req.return_old && previous.is_some() {
        let prev_atom = previous.unwrap();
        match prev_atom {
            Atom::String(s) => set_res.set_old_value(s),
            _ => {}
        }
    }

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = OpType::STR_SET;
    op_res.set_set(set_res);

    op_res
}

pub fn append(db: &mut HashMap<Vec<u8>, Atom>, req: &StrSetOp) -> OpResult {
    // get old value, or use "" if none
    let res = db.get(req.get_key());
    let mut value: Vec<u8> = match res {
        Some(atom) => match atom {
            Atom::String(s) => s.to_vec(),
            _ => {
                return result_err(-400, "not a string");
            }
        },
        None => Vec::new(),
    };

    // begin success result
    let mut set_res = StrSetRes::new();
    if req.return_old {
        set_res.set_old_value(value.clone());
    }

    // append to value
    value.extend_from_slice(req.get_value());
    db.insert(req.get_key().to_vec(), Atom::String(value));

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();
    op_res.ok = true;
    op_res.otype = OpType::STR_APPEND;
    op_res.set_set(set_res);

    op_res
}

#[cfg(test)]
mod tests {
    use crate::string;
    use memds_proto::memds_api::{NumOp, OpType, StrGetOp, StrSetOp};
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
    fn basic_get() {
        let mut db = get_test_db();

        let mut req = StrGetOp::new();
        req.set_key(b"foo".to_vec());
        req.set_want_length(false);

        let res = string::get(&mut db, &req, OpType::STR_GET);

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

        let res = string::get(&mut db, &req, OpType::STR_GET);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_GET);
        assert!(res.has_get());
        assert!(!res.has_set());

        let get_res = res.get_get();
        assert_eq!(get_res.value_length, 3);
    }

    #[test]
    fn get_range() {
        let mut db = get_test_db();

        // testing: (0,4) substr of "Jane Doe"
        let mut req = StrGetOp::new();
        req.set_key(b"name".to_vec());
        req.substr = true;
        req.range_start = 0;
        req.range_end = -4;

        let res = string::get(&mut db, &req, OpType::STR_GETRANGE);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_GETRANGE);
        assert!(res.has_get());
        assert!(!res.has_set());

        let get_res = res.get_get();
        assert_eq!(get_res.value, b"Jane ".to_vec());

        // testing: (0,-1) substr of "Jane Doe"
        let mut req = StrGetOp::new();
        req.set_key(b"name".to_vec());
        req.substr = true;
        req.range_start = 0;
        req.range_end = -1;

        let res = string::get(&mut db, &req, OpType::STR_GETRANGE);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_GETRANGE);
        assert!(res.has_get());
        assert!(!res.has_set());

        let get_res = res.get_get();
        assert_eq!(get_res.value, b"Jane Doe".to_vec());
    }

    #[test]
    fn get_not_found() {
        let mut db = get_test_db();

        let mut req = StrGetOp::new();
        req.set_key(b"does not exist".to_vec());
        req.set_want_length(false);

        let res = string::get(&mut db, &req, OpType::STR_GET);

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

        let res = string::set(&mut db, &req);

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

        let res = string::set(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_SET);
        assert!(!res.has_get());
        assert!(res.has_set());

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"bar".to_vec()); // expect: old value

        let mut req = StrSetOp::new();
        req.set_key(b"foo".to_vec());
        req.set_value(b"door".to_vec());

        let res = string::set(&mut db, &req);

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
        let res = string::incrdecr(&mut db, OpType::STR_INCR, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_INCR);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"0");

        // DECR(num) => 0; old-value==1
        let res = string::incrdecr(&mut db, OpType::STR_DECR, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_DECR);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"1");

        // DECRBY(num,2) => -2; old-value==0
        req.n = 2;
        let res = string::incrdecr(&mut db, OpType::STR_DECRBY, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_DECRBY);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"0");

        // INCRBY(num,2) => 0; old-value==-2
        let res = string::incrdecr(&mut db, OpType::STR_INCRBY, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_INCRBY);
        assert!(res.has_num());

        let num_res = res.get_num();
        assert_eq!(num_res.old_value.to_string().as_bytes(), b"-2");

        // verify final value is indeed 0, from previous operation
        let mut req = StrGetOp::new();
        req.set_key(b"num".to_vec());

        let res = string::get(&mut db, &req, OpType::STR_GET);
        assert_eq!(res.ok, true);
        let get_res = res.get_get();
        assert_eq!(get_res.value, b"0".to_vec());
    }

    #[test]
    fn basic_append() {
        let mut db = get_test_db();

        let mut req = StrSetOp::new();
        req.set_key(b"app".to_vec());
        req.set_value(b"door".to_vec());
        req.set_return_old(true);

        let res = string::append(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_APPEND);
        assert!(!res.has_get());
        assert!(res.has_set());

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"".to_vec()); // expect: old value

        let mut req = StrSetOp::new();
        req.set_key(b"app".to_vec());
        req.set_value(b"door".to_vec());
        req.set_return_old(true);

        let res = string::append(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::STR_APPEND);
        assert!(!res.has_get());
        assert!(res.has_set());

        let set_res = res.get_set();
        assert_eq!(set_res.old_value, b"door".to_vec()); // expect: blank

        // verify final value is indeed doordoor, from previous operation
        let mut req = StrGetOp::new();
        req.set_key(b"app".to_vec());

        let res = string::get(&mut db, &req, OpType::STR_GET);
        assert_eq!(res.ok, true);
        let get_res = res.get_get();
        assert_eq!(get_res.value, b"doordoor".to_vec());
    }
}
