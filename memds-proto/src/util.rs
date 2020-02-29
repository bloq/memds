
use crate::memds_api::{
    MemdsMessage, ResponseMsg, OpResult,
};

pub fn resp_err(code: i32, message: &str) -> MemdsMessage {
    let mut resp = ResponseMsg::new();
    resp.set_ok(false);
    resp.set_err_code(code);
    resp.set_err_message(message.to_string());

    let mut out_msg = MemdsMessage::new();
    out_msg.set_resp(resp);

    out_msg
}

pub fn result_err(code: i32, message: &str) -> OpResult {
    let mut res = OpResult::new();
    res.set_ok(false);
    res.set_err_code(code);
    res.set_err_message(message.to_string());

    res
}

