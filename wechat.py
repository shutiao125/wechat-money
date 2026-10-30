# -*- coding: utf-8 -*-
"""微信公众号接口封装: 签名校验 / XML解析 / 被动回复 / 客服消息 / 模板消息 / 素材下载"""
import time
import json
import hashlib
import threading

import requests
from xml.etree import ElementTree

from config import WECHAT_APPID, WECHAT_SECRET

API = "https://api.weixin.qq.com"


def _post_json(url: str, params: dict, payload: dict):
    """POST JSON: 必须用ensure_ascii=False的UTF-8原文,
    requests默认的\\uXXXX转义会导致微信消息显示为编码串"""
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return requests.post(url, params=params, data=body,
                         headers={"Content-Type": "application/json"}, timeout=10)


# ---------- 回调签名校验 (公众号服务器配置的GET验证) ----------
def check_signature(token: str, signature: str, timestamp: str, nonce: str) -> bool:
    tmp = "".join(sorted([token, timestamp, nonce]))
    return hashlib.sha1(tmp.encode()).hexdigest() == signature


# ---------- XML消息解析 ----------
def parse_xml(xml_bytes: bytes) -> dict:
    """把微信推送的XML解析成dict, 无该字段则返回空dict"""
    try:
        root = ElementTree.fromstring(xml_bytes)
    except ElementTree.ParseError:
        return {}
    return {child.tag: (child.text or "") for child in root}


# ---------- 被动回复XML ----------
def to_text_reply(from_user: str, to_user: str, content: str) -> str:
    from xml.sax.saxutils import escape
    return (
        f"<xml>"
        f"<ToUserName><![CDATA[{escape(to_user)}]]></ToUserName>"
        f"<FromUserName><![CDATA[{escape(from_user)}]]></FromUserName>"
        f"<CreateTime>{int(time.time())}</CreateTime>"
        f"<MsgType><![CDATA[text]]></MsgType>"
        f"<Content><![CDATA[{escape(content)}]]></Content>"
        f"</xml>"
    )


# ---------- access_token 缓存 ----------
_token = {"value": None, "expire": 0}
_lock = threading.Lock()


def get_access_token() -> str:
    with _lock:
        if _token["value"] and _token["expire"] > time.time():
            return _token["value"]
        r = requests.get(f"{API}/cgi-bin/token", params={
            "grant_type": "client_credential",
            "appid": WECHAT_APPID,
            "secret": WECHAT_SECRET,
        }, timeout=10).json()
        if "access_token" not in r:
            raise RuntimeError(f"获取access_token失败: {r}")
        _token["value"] = r["access_token"]
        _token["expire"] = time.time() + r.get("expires_in", 7200) - 300
        return _token["value"]


# ---------- 客服消息 (异步回复: 先回success再主动推送, 规避5秒超时) ----------
def send_custom_text(openid: str, content: str):
    _post_json(f"{API}/cgi-bin/message/custom/send",
               params={"access_token": get_access_token()},
               payload={"touser": openid, "msgtype": "text",
                        "text": {"content": content}})


# ---------- 模板消息 (每日推送) ----------
def send_template(openid: str, template_id: str, data: dict, url: str = ""):
    _post_json(f"{API}/cgi-bin/message/template/send",
               params={"access_token": get_access_token()},
               payload={"touser": openid, "template_id": template_id,
                        "url": url, "data": data})


# ---------- 群发文本 (个人订阅号每天1次, 无需认证) ----------
def send_mass_text(content: str):
    _post_json(f"{API}/cgi-bin/message/mass/sendall",
               params={"access_token": get_access_token()},
               payload={"filter": {"is_to_all": True},
                        "msgtype": "text", "text": {"content": content}})


# ---------- 素材下载 (语音消息转文字兜底用) ----------
def download_media(media_id: str, save_path: str):
    r = requests.get(f"{API}/cgi-bin/media/get",
                     params={"access_token": get_access_token(), "media_id": media_id},
                     timeout=30)
    with open(save_path, "wb") as f:
        f.write(r.content)
    return save_path
