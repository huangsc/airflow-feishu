from airflow.models import variable

from longbridge.operators.feishu import FeishuOperator
from urllib.parse import quote


def failed_alert_by_feishu(context):
    env = variable.Variable.get('env')
    if env != 'prod':
        return
    text = '''AIRFLOW TASK FAILURE TIPS:
'DAG:    {}
'TASK:   {}
'Reason: {}
'''.format(context['task_instance'].dag_id, context['task_instance'].task_id, context['exception'])

    url = f"https://the.url.of.airflow?dag_id={context['task_instance'].dag_id}" \
          f"&root=&execution_date="
    url += quote(f"{context['task_instance'].execution_date}")
    message = {
        'title': '服务告警',
        'tags': [
            {
                'tag': 'text',
                'text': text
            }, {
                'tag': 'a',
                'text': '查看详情',
                'href': url,
            }
        ]
    }

    return FeishuOperator(
        task_id='failed_alert',
        feishu_conn_id='example',
        message_type='post',
        message=message,
        secret=True,
    ).execute(context)
