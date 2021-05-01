from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from longbridge.hooks.feishu import FeishuHook


class FeishuOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        *,
        feishu_conn_id: str = 'feishu_default',
        message_type: str = 'text',
        message: str = '',
        secret: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.feishu_conn_id = feishu_conn_id
        self.message_type = message_type
        self.message = message
        self.secret = secret

    def execute(self, context) -> None:
        self.log.info('Sending feishu message.')
        hook = FeishuHook(
            self.feishu_conn_id, self.message_type, self.message, self.secret
        )
        hook.send()
