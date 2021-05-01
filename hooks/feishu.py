import base64
import hmac
import json
import time
from hashlib import sha256
from pprint import pprint
from typing import List, Optional, Union

import requests
from requests import Session

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class FeishuHook(HttpHook):

    def __init__(
        self,
        feishu_conn_id='feishu_default',
        message_type: str = 'text',
        message: Optional[Union[str, dict]] = None,
        secret: bool = False,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(http_conn_id=feishu_conn_id, *args, **kwargs) 
        self.message_type = message_type
        self.message = message
        self.secret = secret

    def _get_sign(self, timestamp) -> str:
        conn = self.get_connection(self.http_conn_id)
        secret = conn.password
        if not secret:
            raise AirflowException(
                'secret is requests but get nothing, check you conn_id configuration.'
            )
        key = f'{timestamp}\n{secret}'
        key_enc = key.encode('utf-8')
        msg = ""
        msg_enc = msg.encode('utf-8')
        hmac_code = hmac.new(key_enc, msg_enc, digestmod=sha256).digest()
        sign = base64.b64encode(hmac_code).decode('utf-8')
        return sign

    def _build_message(self) -> str:
        if self.secret:
            timestamp = str(int(time.time()))
            sign = self._get_sign(timestamp)
            if self.message_type == 'text':
                data = {
                    'timestamp': timestamp,
                    'sign': sign,
                    'msg_type': self.message_type,
                    'content': {'text': self.message},
                }
            elif self.message_type == 'post':
                if not isinstance(self.message, dict):
                    raise TypeError('message must be dict')
                data = {
                    'timestamp': timestamp,
                    'sign': sign,
                    'msg_type': self.message_type,
                    'content': {
                        'post': {
                            'zh_cn': {
                                'title': self.message['title'],
                                'content': [
                                    self.message['tags']
                                ]
                            }
                        }
                    },
                }
        else:
            if self.message_type == 'text':
                data = {
                    'msg_type': self.message_type,
                    'content': {'text': self.message},
                }
            elif self.message_type == 'post':
                if not isinstance(self.message, dict):
                    raise TypeError('message must be dict')
                data = {
                    'msg_type': self.message_type,
                    'content': {
                        'post': {
                            'zh_cn': {
                                'title': self.message['title'],
                                'content': [
                                    self.message['tags']
                                ]
                            }
                        }
                    },
                }
        return json.dumps(data)

    def get_conn(self, headers: Optional[dict] = None) -> Session:
        conn = self.get_connection(self.http_conn_id)
        if not conn.host:
            raise ValueError(
                'FeishuWebhookHook host is None'
            )
        self.base_url = conn.host
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session

    def send(self) -> None:
        # 飞书目前有五种消息类型【'text', 'post', 'image', 'share_chat', 'interactive'】，这里暂时只支持text和post
        support_type = ['text', 'post']
        if self.message_type not in support_type:
            raise ValueError(
                'FeishuWebhookHook only support {} '
                'so far, but receive {}'.format(support_type, self.message_type)
            )

        data = self._build_message()
        self.log.info('Sending Feishu type %s message %s', self.message_type, data)
        resp = self.run(endpoint='', data=data, headers={'Content-Type': 'application/json'})
        pprint(resp.json())
        code = resp.json().get('StatusCode')
        if code is None or int(code) != 0:
            raise AirflowException('Send Feishu message failed')
        self.log.info('Success Send Feishu message')
