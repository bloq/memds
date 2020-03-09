use std::collections::HashMap;

use memds_proto::memds_api::{
    AtomType, CountRes, KeyListOp, KeyOp, KeyRenameOp, OpResult, OpType, TypeRes,
};
use memds_proto::util::result_err;
use memds_proto::Atom;

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

#[cfg(test)]
mod tests {
    use crate::{keys, string};
    use memds_proto::memds_api::{AtomType, KeyListOp, KeyOp, KeyRenameOp, OpType, StrGetOp};
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
}
