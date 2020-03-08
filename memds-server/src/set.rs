use std::collections::HashMap;
use std::collections::HashSet;

use memds_proto::memds_api::{CountRes, KeyOp, KeyedListOp, ListRes, OpResult, OpType, SetInfoRes};
use memds_proto::util::result_err;
use memds_proto::Atom;

pub fn add_del(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyedListOp, otype: OpType) -> OpResult {
    let st = {
        let key = req.get_key();
        match db.get_mut(key) {
            None => {
                db.insert(key.to_vec(), Atom::Set(HashSet::new()));
                match db.get_mut(key) {
                    None => unreachable!(),
                    Some(atom) => match atom {
                        Atom::Set(st) => st,
                        _ => unreachable!(),
                    },
                }
            }
            Some(atom) => match atom {
                Atom::Set(st) => st,
                _ => {
                    return result_err(-400, "not a list");
                }
            },
        }
    };

    let do_delete = match otype {
        OpType::SET_DEL => true,
        _ => false,
    };

    let mut n_updates = 0;
    for element in req.elements.iter() {
        if do_delete {
            if st.remove(element) {
                n_updates += 1;
            }
        } else {
            if st.insert(element.to_vec()) {
                n_updates += 1;
            }
        }
    }

    let mut count_res = CountRes::new();
    count_res.n = n_updates as u64;

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = otype;
    op_res.set_count(count_res);

    op_res
}

pub fn info(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    let st = {
        let key = req.get_key();
        match db.get(key) {
            None => {
                return result_err(-404, "Not Found");
            }
            Some(atom) => match atom {
                Atom::Set(st) => st,
                _ => {
                    return result_err(-400, "not a set");
                }
            },
        }
    };

    let mut info_res = SetInfoRes::new();
    info_res.length = st.len() as u32;

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_INFO;
    op_res.set_set_info(info_res);

    op_res
}

pub fn members(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    let st = {
        let key = req.get_key();
        match db.get(key) {
            None => {
                return result_err(-404, "Not Found");
            }
            Some(atom) => match atom {
                Atom::Set(st) => st,
                _ => {
                    return result_err(-400, "not a set");
                }
            },
        }
    };

    let mut list_res = ListRes::new();
    for item in st.iter() {
        list_res.elements.push(item.to_vec());
    }

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_MEMBERS;
    op_res.set_list(list_res);

    op_res
}

pub fn is_member(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyedListOp) -> OpResult {
    let st = {
        let key = req.get_key();
        match db.get(key) {
            None => {
                return result_err(-404, "Not Found");
            }
            Some(atom) => match atom {
                Atom::Set(st) => st,
                _ => {
                    return result_err(-400, "not a set");
                }
            },
        }
    };

    let mut n_match = 0;
    for item in req.elements.iter() {
        if st.contains(item) {
            n_match += 1;
        }
    }

    let mut count_res = CountRes::new();
    count_res.n = n_match;

    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_ISMEMBER;
    op_res.set_count(count_res);

    op_res
}

#[cfg(test)]
mod tests {
    use crate::set;
    use memds_proto::memds_api::{KeyOp, KeyedListOp, OpType};
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
    fn add() {
        let mut db = get_test_db();

        // add one,two,two == set(one,two)
        let mut req = KeyedListOp::new();
        req.set_key(b"a_set".to_vec());
        req.elements.push(b"one".to_vec());
        req.elements.push(b"two".to_vec());
        req.elements.push(b"two".to_vec());

        let res = set::add_del(&mut db, &req, OpType::SET_ADD);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_ADD);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);

        // get set info, verify count again
        let mut req = KeyOp::new();
        req.set_key(b"a_set".to_vec());

        let res = set::info(&mut db, &req);
        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_INFO);
        assert!(res.has_set_info());

        let info_res = res.get_set_info();
        assert_eq!(info_res.length, 2);
    }

    #[test]
    fn del() {
        let mut db = get_test_db();

        // add one,two,two == set(one,two)
        let mut req = KeyedListOp::new();
        req.set_key(b"a_set".to_vec());
        req.elements.push(b"one".to_vec());
        req.elements.push(b"two".to_vec());
        req.elements.push(b"two".to_vec());

        let res = set::add_del(&mut db, &req, OpType::SET_ADD);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_ADD);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);

        // del one == set(two)
        let mut req = KeyedListOp::new();
        req.set_key(b"a_set".to_vec());
        req.elements.push(b"one".to_vec());

        let res = set::add_del(&mut db, &req, OpType::SET_DEL);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_DEL);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 1);

        // get set info, verify count again
        let mut req = KeyOp::new();
        req.set_key(b"a_set".to_vec());

        let res = set::info(&mut db, &req);
        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_INFO);
        assert!(res.has_set_info());

        let info_res = res.get_set_info();
        assert_eq!(info_res.length, 1);
    }

    #[test]
    fn members() {
        let mut db = get_test_db();

        // add one,two,two == set(one,two)
        let mut req = KeyedListOp::new();
        req.set_key(b"a_set".to_vec());
        req.elements.push(b"one".to_vec());
        req.elements.push(b"two".to_vec());
        req.elements.push(b"two".to_vec());

        let res = set::add_del(&mut db, &req, OpType::SET_ADD);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_ADD);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);

        // get set info, verify count again
        let mut req = KeyOp::new();
        req.set_key(b"a_set".to_vec());

        let mut res = set::members(&mut db, &req);
        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_MEMBERS);
        assert!(res.has_list());

        let list_res = res.mut_list();
        list_res.elements.sort();
        assert_eq!(list_res.elements.len(), 2);
        assert_eq!(list_res.elements[0], b"one");
        assert_eq!(list_res.elements[1], b"two");
    }

    #[test]
    fn is_member() {
        let mut db = get_test_db();

        // add one,two,two == set(one,two)
        let mut req = KeyedListOp::new();
        req.set_key(b"a_set".to_vec());
        req.elements.push(b"one".to_vec());
        req.elements.push(b"two".to_vec());
        req.elements.push(b"two".to_vec());

        let res = set::add_del(&mut db, &req, OpType::SET_ADD);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_ADD);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);

        // get set info, verify count again
        let mut req = KeyedListOp::new();
        req.set_key(b"a_set".to_vec());
        req.elements.push(b"one".to_vec());
        req.elements.push(b"does-not-exist".to_vec());

        let res = set::is_member(&mut db, &req);
        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_ISMEMBER);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 1);
    }
}
