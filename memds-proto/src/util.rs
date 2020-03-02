use crate::memds_api::OpResult;

pub fn result_err(code: i32, message: &str) -> OpResult {
    let mut res = OpResult::new();
    res.set_ok(false);
    res.set_err_code(code);
    res.set_err_message(message.to_string());

    res
}
