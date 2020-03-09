use std::collections::HashMap;
use std::collections::HashSet;

use memds_proto::memds_api::{
    CmpStoreOp, CountRes, KeyOp, KeyedListOp, ListRes, OpResult, OpType, SetInfoRes,
};
use memds_proto::util::result_err;
use memds_proto::Atom;

pub fn add_del(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyedListOp, otype: OpType) -> OpResult {
    let do_delete = match otype {
        OpType::SET_DEL => true,
        _ => false,
    };

    // get set to mutate.
    let st = {
        let key = req.get_key();
        match db.get_mut(key) {
            None => {
                // if we're deleting, go no further
                if do_delete {
                    return result_err(-404, "Not Found");
                }

                // does not exist; create empty set
                db.insert(key.to_vec(), Atom::Set(HashSet::new()));
                match db.get_mut(key) {
                    None => unreachable!(),
                    Some(atom) => match atom {
                        Atom::Set(st) => st,
                        _ => unreachable!(),
                    },
                }
            }

            // found the key.  grab ref.
            Some(atom) => match atom {
                Atom::Set(st) => st,
                _ => {
                    return result_err(-400, "not a list");
                }
            },
        }
    };

    // iterate through each element in request, adding/deleting
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

    // return number of updates (not number of elements)
    let mut count_res = CountRes::new();
    count_res.n = n_updates as u64;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = otype;
    op_res.set_count(count_res);

    op_res
}

pub fn info(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    // get set to query
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

    // return set info aka metadata.   at present, just the element count.
    let mut info_res = SetInfoRes::new();
    info_res.length = st.len() as u32;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_INFO;
    op_res.set_set_info(info_res);

    op_res
}

pub fn members(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    // get set to query
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

    // push entire set contents into result.
    // todo: there is probably a Rust-y way to .zip() or .collect() here
    let mut list_res = ListRes::new();
    for item in st.iter() {
        list_res.elements.push(item.to_vec());
    }

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_MEMBERS;
    op_res.set_list(list_res);

    op_res
}

pub fn diff(db: &mut HashMap<Vec<u8>, Atom>, req: &CmpStoreOp) -> OpResult {
    if req.keys.len() < 1 {
        return result_err(-400, "at least one key required");
    }

    // iterate through list of provided keys
    let mut diff_result = HashSet::new();
    let mut first_key = true;
    for key in req.keys.iter() {
        // first key: read set, or empty set upon exception
        if first_key {
            first_key = false;

            let atom_res = db.get(key);
            match atom_res {
                Some(Atom::Set(st)) => {
                    diff_result = st.clone();
                }
                _ => {
                    diff_result = HashSet::new();
                }
            }

        // following keys: attempt to remove from difference set
        } else {
            let atom_res = db.get(key);
            let operand = match atom_res {
                Some(Atom::Set(st)) => st.clone(),
                _ => HashSet::new(),
            };

            for oper_elem in operand.iter() {
                diff_result.remove(oper_elem);
            }
        }
    }

    // standard operation result assignment
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_DIFF;

    let do_store = req.store_key.len() > 0;

    // if storing in db, do so + return count stored
    if do_store {
        let n_results = diff_result.len() as u64;
        db.insert(req.store_key.to_vec(), Atom::Set(diff_result));

        let mut count_res = CountRes::new();
        count_res.n = n_results;
        op_res.set_count(count_res);

    // otherwise return calculated result directly to client
    } else {
        let mut list_res = ListRes::new();
        for elem in diff_result.iter() {
            list_res.elements.push(elem.to_vec());
        }
        op_res.set_list(list_res);
    }

    op_res
}

pub fn union(db: &mut HashMap<Vec<u8>, Atom>, req: &CmpStoreOp) -> OpResult {
    if req.keys.len() < 1 {
        return result_err(-400, "at least one key required");
    }

    // iterate through list of provided keys
    // inserting into result union
    let mut diff_result = HashSet::new();
    for key in req.keys.iter() {
        match db.get(key) {
            Some(Atom::Set(st)) => {
                for elem in st.iter() {
                    diff_result.insert(elem.clone());
                }
            }
            _ => {}
        }
    }

    // standard operation result assignment
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_UNION;

    let do_store = req.store_key.len() > 0;

    // if storing in db, do so + return count stored
    if do_store {
        let n_results = diff_result.len() as u64;
        db.insert(req.store_key.to_vec(), Atom::Set(diff_result));

        let mut count_res = CountRes::new();
        count_res.n = n_results;
        op_res.set_count(count_res);

    // otherwise return calculated result directly to client
    } else {
        let mut list_res = ListRes::new();
        for elem in diff_result.iter() {
            list_res.elements.push(elem.to_vec());
        }
        op_res.set_list(list_res);
    }

    op_res
}

pub fn is_member(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyedListOp) -> OpResult {
    // get set to query
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

    // count number of matches of set intersecting with provided list
    let mut n_match = 0;
    for item in req.elements.iter() {
        if st.contains(item) {
            n_match += 1;
        }
    }

    // return matched count
    let mut count_res = CountRes::new();
    count_res.n = n_match;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::SET_ISMEMBER;
    op_res.set_count(count_res);

    op_res
}

#[cfg(test)]
mod tests {
    use crate::set;
    use memds_proto::memds_api::{CmpStoreOp, KeyOp, KeyedListOp, OpType};
    use memds_proto::Atom;
    use std::collections::HashMap;
    use std::collections::HashSet;

    fn get_test_db() -> HashMap<Vec<u8>, Atom> {
        let mut db: HashMap<Vec<u8>, Atom> = HashMap::new();
        db.insert(b"foo".to_vec(), Atom::String(b"bar".to_vec()));
        db.insert(b"name".to_vec(), Atom::String(b"Jane Doe".to_vec()));
        db.insert(b"age".to_vec(), Atom::String(b"25".to_vec()));

        let mut st = HashSet::new();
        st.insert(b"a".to_vec());
        st.insert(b"b".to_vec());
        st.insert(b"c".to_vec());
        st.insert(b"d".to_vec());
        db.insert(b"set1".to_vec(), Atom::Set(st));

        let mut st = HashSet::new();
        st.insert(b"c".to_vec());
        db.insert(b"set2".to_vec(), Atom::Set(st));

        let mut st = HashSet::new();
        st.insert(b"a".to_vec());
        st.insert(b"c".to_vec());
        st.insert(b"e".to_vec());
        db.insert(b"set3".to_vec(), Atom::Set(st));

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

    #[test]
    fn diff() {
        let mut db = get_test_db();

        // add one,two,two == set(one,two)
        let mut req = CmpStoreOp::new();
        req.keys.push(b"set1".to_vec());
        req.keys.push(b"set2".to_vec());
        req.keys.push(b"set3".to_vec());

        let mut res = set::diff(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_DIFF);
        assert!(res.has_list());

        let list_res = res.mut_list();
        list_res.elements.sort();
        assert_eq!(list_res.elements.len(), 2);
        assert_eq!(list_res.elements[0], b"b");
        assert_eq!(list_res.elements[1], b"d");
    }

    #[test]
    fn union() {
        let mut db = get_test_db();

        // add one,two,two == set(one,two)
        let mut req = CmpStoreOp::new();
        req.keys.push(b"set1".to_vec());
        req.keys.push(b"set2".to_vec());
        req.keys.push(b"set3".to_vec());

        let mut res = set::union(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::SET_UNION);
        assert!(res.has_list());

        let list_res = res.mut_list();
        list_res.elements.sort();
        assert_eq!(list_res.elements.len(), 5);
        assert_eq!(list_res.elements[0], b"a");
        assert_eq!(list_res.elements[1], b"b");
        assert_eq!(list_res.elements[2], b"c");
        assert_eq!(list_res.elements[3], b"d");
        assert_eq!(list_res.elements[4], b"e");
    }
}
