use std::collections::HashMap;
use std::collections::HashSet;

use bytes::{BufMut, Bytes, BytesMut};
use memds_proto::memds_api::{
    AtomType, CountRes, DbValue, KeyListOp, KeyOp, KeyRenameOp, MemdsMessage, MemdsMessage_MsgType,
    OpResult, OpType, StrGetRes, StrSetOp, TypeRes,
};
use memds_proto::util::result_err;
use memds_proto::{Atom, MemdsCodec};
use tokio_util::codec::{Decoder, Encoder};

pub fn del_exist(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyListOp, remove_item: bool) -> OpResult {
    let mut count: u64 = 0;

    // iterate through provided key list
    for key in req.get_keys().iter() {
        // if we're deleting, attempt to remove item
        if remove_item {
            if db.remove(key).is_some() {
                count += 1;
            }

        // if we're testing existence, do so
        } else {
            if db.contains_key(key) {
                count += 1;
            }
        }
    }

    // return number of keys matched (== operations successful, for delete)
    let mut count_res = CountRes::new();
    count_res.n = count;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = match remove_item {
        true => OpType::KEYS_DEL,
        false => OpType::KEYS_EXIST,
    };
    op_res.set_count(count_res);

    op_res
}

pub fn rename(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyRenameOp) -> OpResult {
    let old_key = req.get_old_key();
    let new_key = req.get_new_key();

    if req.create_excl && db.contains_key(new_key) {
        return result_err(-412, "Precondition failed: key exists");
    }

    // remove value stored at old key
    let value = {
        let rm_res = db.remove(old_key);
        if rm_res.is_none() {
            return result_err(-404, "Not Found");
        }

        rm_res.unwrap()
    };

    // store value at new key
    db.insert(new_key.to_vec(), value);

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::KEYS_RENAME;

    op_res
}

pub fn typ(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    let key = req.get_key();

    // get value stored at key
    let typ = match db.get(key) {
        None => {
            return result_err(-404, "Not Found");
        }

        // match type
        Some(atom) => match atom {
            Atom::String(_) => AtomType::STRING,
            Atom::List(_) => AtomType::LIST,
            Atom::Set(_) => AtomType::SET,
        },
    };

    // return type
    let mut type_res = TypeRes::new();
    type_res.typ = typ;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::KEYS_TYPE;
    op_res.set_typ(type_res);

    op_res
}

pub fn element_dbv(db: &HashMap<Vec<u8>, Atom>, key: &[u8]) -> Option<DbValue> {
    // create result DbValue
    let mut dbv = DbValue::new();
    dbv.set_key(key.to_vec());

    // encode DbValue value
    match db.get(key) {
        None => return None,

        // match type
        Some(atom) => match atom {
            Atom::String(s) => {
                dbv.typ = AtomType::STRING;
                dbv.set_str(s.clone());
            }
            Atom::List(l) => {
                dbv.typ = AtomType::LIST;
                for elem in l.iter() {
                    dbv.elements.push(elem.clone());
                }
            }
            Atom::Set(st) => {
                dbv.typ = AtomType::SET;
                for elem in st.iter() {
                    dbv.elements.push(elem.clone());
                }
            }
        },
    };

    Some(dbv)
}

pub fn dump(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    // create result DbValue
    let dbv = {
        match element_dbv(db, req.get_key()) {
            None => {
                return result_err(-404, "Not Found");
            }
            Some(dbv) => dbv,
        }
    };

    // encode DbValue wrapper message
    let mut msg = MemdsMessage::new();
    msg.mtype = MemdsMessage_MsgType::DBVAL;
    msg.set_dbv(dbv);

    // encode to wire protocol using tokio codec
    let mut codec = MemdsCodec::new();
    let msg_raw = &mut BytesMut::new();
    codec.encode(msg, msg_raw).unwrap();

    // encode wire bytes into StrGetRes result bytes
    let mut get_res = StrGetRes::new();
    get_res.set_value(msg_raw.to_vec());

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::KEY_DUMP;
    op_res.set_get(get_res);

    op_res
}

pub fn restore(db: &mut HashMap<Vec<u8>, Atom>, req: &StrSetOp) -> OpResult {
    let msg = {
        let mut codec = MemdsCodec::new();
        let buf = Bytes::from(req.value.clone());
        let msg_raw = &mut BytesMut::new();
        msg_raw.put(buf);
        match codec.decode(msg_raw) {
            Err(_) => {
                return result_err(-400, "Deser failed");
            }
            Ok(None) => {
                return result_err(-400, "Deser empty");
            }
            Ok(Some(dec_msg)) => {
                if (dec_msg.mtype != MemdsMessage_MsgType::DBVAL) || (!dec_msg.has_dbv()) {
                    return result_err(-400, "not dbv");
                }

                dec_msg
            }
        }
    };
    let dbv = msg.get_dbv();
    let key = {
        if req.key.len() > 0 {
            &req.key
        } else {
            &dbv.key
        }
    };
    let value = match dbv.typ {
        AtomType::NOTYPE => Atom::String(b"".to_vec()),
        AtomType::STRING => Atom::String(dbv.get_str().to_vec()),
        AtomType::LIST => {
            let mut v = Vec::new();
            for elem in dbv.elements.iter() {
                v.push(elem.to_vec());
            }
            Atom::List(v)
        }
        AtomType::SET => {
            let mut hs = HashSet::new();
            for elem in dbv.elements.iter() {
                hs.insert(elem.to_vec());
            }
            Atom::Set(hs)
        }
    };

    db.insert(key.to_vec(), value);

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::KEY_RESTORE;

    op_res
}

#[cfg(test)]
mod tests {
    use crate::{keys, string};
    use memds_proto::memds_api::{
        AtomType, KeyListOp, KeyOp, KeyRenameOp, OpType, StrGetOp, StrSetOp,
    };
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
    fn del() {
        let mut db = get_test_db();

        let mut req = KeyListOp::new();
        req.keys.push(b"foo".to_vec());
        req.keys.push(b"age".to_vec());
        req.keys.push(b"does-not-exist".to_vec());

        let res = keys::del_exist(&mut db, &req, true);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEYS_DEL);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);
    }

    #[test]
    fn exist() {
        let mut db = get_test_db();

        // count=2 keys of 3 in test set
        let mut req = KeyListOp::new();
        req.keys.push(b"foo".to_vec());
        req.keys.push(b"age".to_vec());
        req.keys.push(b"does-not-exist".to_vec());

        let res = keys::del_exist(&mut db, &req, false);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEYS_EXIST);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);

        // repeat same test, to make sure keys did not disappear
        let mut req = KeyListOp::new();
        req.keys.push(b"foo".to_vec());
        req.keys.push(b"age".to_vec());
        req.keys.push(b"does-not-exist".to_vec());

        let res = keys::del_exist(&mut db, &req, false);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEYS_EXIST);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);
    }

    #[test]
    fn typ() {
        let mut db = get_test_db();

        let mut req = KeyOp::new();
        req.set_key(b"foo".to_vec());

        let res = keys::typ(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEYS_TYPE);
        assert!(res.has_typ());

        let type_res = res.get_typ();
        assert_eq!(type_res.typ, AtomType::STRING);
    }

    #[test]
    fn rename() {
        let mut db = get_test_db();

        // rename "foo" to "food"
        let mut req = KeyRenameOp::new();
        req.set_old_key(b"foo".to_vec());
        req.set_new_key(b"food".to_vec());
        req.create_excl = true;

        let res = keys::rename(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEYS_RENAME);

        // get "foo" == not found
        let mut req = StrGetOp::new();
        req.set_key(b"foo".to_vec());

        let res = string::get(&mut db, &req, OpType::STR_GET);

        assert_eq!(res.ok, false);
        assert_eq!(res.otype, OpType::NOOP);
        assert_eq!(res.err_code, -404);

        // get "food" == found
        let mut req = StrGetOp::new();
        req.set_key(b"food".to_vec());

        let res = string::get(&mut db, &req, OpType::STR_GET);

        assert_eq!(res.ok, true);

        let get_res = res.get_get();
        assert_eq!(get_res.value, b"bar".to_vec());
    }

    #[test]
    fn dump_string() {
        let mut db = get_test_db();

        // dump (serialize) string
        let mut req = KeyOp::new();
        req.set_key(b"foo".to_vec());

        let res = keys::dump(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEY_DUMP);
        assert!(res.has_get());

        let get_res = res.get_get();
        let enc_wire_data = get_res.get_value();

        // restore to different key, and compare
        let mut set_req = StrSetOp::new();
        set_req.set_key(b"foo2".to_vec());
        set_req.set_value(enc_wire_data.to_vec());

        let res = keys::restore(&mut db, &set_req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::KEY_RESTORE);

        // get "foo2" == "bar"
        let mut req = StrGetOp::new();
        req.set_key(b"foo2".to_vec());

        let res = string::get(&mut db, &req, OpType::STR_GET);

        assert_eq!(res.ok, true);

        let get_res = res.get_get();
        assert_eq!(get_res.value, b"bar".to_vec());
    }
}
