use std::collections::HashMap;

use memds_proto::memds_api::{
    CountRes, KeyOp, ListIndexOp, ListInfoRes, ListPopOp, ListPushOp, ListRes, OpResult, OpType,
};
use memds_proto::util::result_err;
use memds_proto::Atom;

pub fn info(db: &mut HashMap<Vec<u8>, Atom>, req: &KeyOp) -> OpResult {
    // get list to query
    let l = {
        let key = req.get_key();
        match db.get_mut(key) {
            None => {
                return result_err(-404, "Not Found");
            }
            Some(atom) => match atom {
                Atom::List(l) => l,
                _ => {
                    return result_err(-400, "not a list");
                }
            },
        }
    };

    // return list info (aka list metadata)
    // at present there is only item count (list length),
    // but more metadata is presumed in the future.
    let mut info_res = ListInfoRes::new();
    info_res.length = l.len() as u32;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::LIST_INFO;
    op_res.set_list_info(info_res);

    op_res
}

pub fn push(db: &mut HashMap<Vec<u8>, Atom>, req: &ListPushOp) -> OpResult {
    // get list to mutate
    let l = {
        let key = req.get_key();
        match db.get_mut(key) {
            None => {
                if req.if_exists {
                    return result_err(-404, "Not Found");
                }
                db.insert(key.to_vec(), Atom::List(Vec::new()));
                match db.get_mut(key) {
                    None => unreachable!(),
                    Some(atom) => match atom {
                        Atom::List(l) => l,
                        _ => unreachable!(),
                    },
                }
            }
            Some(atom) => match atom {
                Atom::List(l) => l,
                _ => {
                    return result_err(-400, "not a list");
                }
            },
        }
    };

    // insert, at head or tail as requested
    if req.at_head {
        for element in req.elements.iter() {
            l.insert(0, element.to_vec());
        }
    } else {
        for element in req.elements.iter() {
            l.insert(l.len(), element.to_vec());
        }
    }

    // return new list length, after mutations (if any)
    let mut count_res = CountRes::new();
    count_res.n = l.len() as u64;

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::LIST_PUSH;
    op_res.set_count(count_res);

    op_res
}

pub fn pop(db: &mut HashMap<Vec<u8>, Atom>, req: &ListPopOp) -> OpResult {
    // get list to mutate
    let l = {
        let key = req.get_key();
        match db.get_mut(key) {
            None => {
                return result_err(-404, "Not Found");
            }
            Some(atom) => match atom {
                Atom::List(l) => l,
                _ => {
                    return result_err(-400, "not a list");
                }
            },
        }
    };

    let mut list_res = ListRes::new();

    // remove, and return removed values, at head or tail as requested
    if l.len() > 0 {
        let value = {
            if req.at_head {
                l.remove(0)
            } else {
                l.pop().unwrap()
            }
        };

        list_res.elements.push(value);
    }

    // standard operation result assignment & final return
    let mut op_res = OpResult::new();

    op_res.ok = true;
    op_res.otype = OpType::LIST_POP;
    op_res.set_list(list_res);

    op_res
}

pub fn index(db: &mut HashMap<Vec<u8>, Atom>, req: &ListIndexOp) -> OpResult {
    // get list to query
    match db.get(req.get_key()) {
        Some(atom) => match atom {
            Atom::List(l) => {
                let mut index_res = ListRes::new();

                // get absolute pos, calculating from absolute (non-neg)
                // or relative (negative) supplied positions.
                let pos = {
                    if req.index < 0 {
                        let tmp: i64 = (l.len() as i64) + req.index as i64;
                        if tmp < 0 {
                            0
                        } else {
                            tmp as usize
                        }
                    } else {
                        req.index as usize
                    }
                };

                // return element, if position valid
                if pos < l.len() {
                    index_res.elements.push(l[pos].clone());
                }

                // standard operation result assignment & final return
                let mut op_res = OpResult::new();
                op_res.ok = true;
                op_res.otype = OpType::LIST_INDEX;
                op_res.set_list(index_res);

                op_res
            }
            _ => result_err(-400, "not a list"),
        },
        None => result_err(-404, "Not Found"),
    }
}

#[cfg(test)]
mod tests {
    use crate::list;
    use memds_proto::memds_api::{KeyOp, ListIndexOp, ListPopOp, ListPushOp, OpType};
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
    fn basic() {
        let mut db = get_test_db();

        // push 1 item onto empty list

        let mut req = ListPushOp::new();
        req.set_key(b"lst".to_vec());
        req.elements.push(b"two".to_vec());

        let res = list::push(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_PUSH);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 1);

        // push 1 item onto list head
        let mut req = ListPushOp::new();
        req.set_key(b"lst".to_vec());
        req.at_head = true;
        req.elements.push(b"one".to_vec());

        let res = list::push(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_PUSH);
        assert!(res.has_count());

        let count_res = res.get_count();
        assert_eq!(count_res.n, 2);

        // verify index 0 == "one"
        let mut req = ListIndexOp::new();
        req.set_key(b"lst".to_vec());
        req.index = 0;

        let res = list::index(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_INDEX);
        assert!(res.has_list());

        let list_res = res.get_list();
        assert_eq!(list_res.elements.len(), 1);
        assert_eq!(list_res.elements[0], b"one");

        // verify index 1 == "two"
        let mut req = ListIndexOp::new();
        req.set_key(b"lst".to_vec());
        req.index = 1;

        let res = list::index(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_INDEX);
        assert!(res.has_list());

        let list_res = res.get_list();
        assert_eq!(list_res.elements.len(), 1);
        assert_eq!(list_res.elements[0], b"two");

        // verify index -1 == "two"
        let mut req = ListIndexOp::new();
        req.set_key(b"lst".to_vec());
        req.index = -1;

        let res = list::index(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_INDEX);
        assert!(res.has_list());

        let list_res = res.get_list();
        assert_eq!(list_res.elements.len(), 1);
        assert_eq!(list_res.elements[0], b"two");

        // verify pop() == "two"
        let mut req = ListPopOp::new();
        req.set_key(b"lst".to_vec());

        let res = list::pop(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_POP);
        assert!(res.has_list());

        let list_res = res.get_list();
        assert_eq!(list_res.elements.len(), 1);
        assert_eq!(list_res.elements[0], b"two");

        // verify with list metadata
        let mut req = KeyOp::new();
        req.set_key(b"lst".to_vec());

        let res = list::info(&mut db, &req);

        assert_eq!(res.ok, true);
        assert_eq!(res.otype, OpType::LIST_INFO);
        assert!(res.has_list_info());

        let info_res = res.get_list_info();
        assert_eq!(info_res.length, 1);
    }
}
