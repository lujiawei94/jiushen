from dingtalkchatbot.chatbot import DingtalkChatbot
from loguru import logger

class DingDing:

    Webhook = 'https://oapi.dingtalk.com/robot/send?access_token=3a6a6cd6649247bfa95f0e5373e56d98253822c5dd8eb72bf88872ebb3817280'

    @classmethod
    def postDinDin(cls, error_msg, webhook=None, is_at_all=False, at_dingtalk_ids=None):
        if at_dingtalk_ids is None:
            at_dingtalk_ids = ['12h-2gyss0pcmx', "hsy0j57"]
        webhook = cls.Webhook if not webhook else webhook
        logger.debug(f'post_dingding: {error_msg}')
        xiaodingding = DingtalkChatbot(webhook)
        xiaodingding.send_text(msg=error_msg, is_at_all=is_at_all, at_dingtalk_ids=at_dingtalk_ids)
