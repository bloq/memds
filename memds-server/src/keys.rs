use std::collections::HashMap;

use memds_proto::memds_api::{CountRes, KeyListOp, OpResult, OpType};
use memds_proto::Atom;

pub fn del_exist(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyListOp, remove_item: bool) -> OpResult {
    let mut count: u64 = 0;

    for key in req.get_keys().iter() {
        if remove_item {
            if db.remove(key).is_some() {
                count += 1;
            }
        } else {
            if db.contains_key(key) {
                count += 1;
            }
        }
    }

    let mut count_res = CountRes::new();
    count_res.n = count;

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = match remove_item {
        true => OpType::KEYS_DEL,
        false => OpType::KEYS_EXIST,
    };
    op_res.set_count(count_res);

    op_res
}

#[cfg(test)]
mod tests {
    use crate::keys;
    use memds_proto::memds_api::{KeyListOp, OpType};
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
}
