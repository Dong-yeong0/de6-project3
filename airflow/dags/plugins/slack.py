from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def send_message(slack_msg, context, title, color):
    return SlackWebhookOperator(
        slack_webhook_conn_id='SLACK_WEBHOOK',
        task_id='slack_alert',
        message=slack_msg,
        username='airflow',
        attachments=[
            {
                'mrkdwn_in': ['text'],
                'title': title,
                'actions': [
                    {
                        'type': 'button',
                        'name': 'view log',
                        'text': 'View log',
                        'url': context['task_instance'].log_url,
                        'style': 'danger' if color == 'danger' else 'default',
                    },
                ],
                'color': color,  # 'good', 'warning', 'danger', or hex ('#439FE0')
                'fallback': 'details',  # Required plain-text summary of the attachment
            }
        ]
    )


def send_fail_alert(context):
    slack_msg = f'''
    :red_circle: Task Failed
    *Dag*: {context['ti'].dag_id}
    *Task*: {context['ti'].task_id}
    *Execution Time*: {context['logical_date']}
    '''
    alert = send_message(slack_msg, context, title='Task Failure', color='danger')

    return alert.execute(context=context)
