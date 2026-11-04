import os
import asyncio
import httpx
from fastapi import FastAPI, Request

app = FastAPI()

# 客户消息暂存区
user_buffers = {}

# 自动读取环境变量里的 Make Webhook 地址
MAKE_WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL", "")

async def delay_and_send(chat_id: str):
    try:
        # 静默等待 30 秒（等买家发完连续短句）
        await asyncio.sleep(30)
        
        data = user_buffers.pop(chat_id, None)
        if not data:
            return
            
        combined_text = "\n".join(data["messages"])
        
        # 将打包合并好的整段内容推送到 Make.com
        if MAKE_WEBHOOK_URL:
            async with httpx.AsyncClient() as client:
                payload = {
                    "chatId": chat_id,
                    "textMessage": combined_text
                }
                await client.post(MAKE_WEBHOOK_URL, json=payload, timeout=30.0)
                
    except asyncio.CancelledError:
        # 15秒内客户又有新打字进线，取消上一轮，重置倒计时
        pass

@app.get("/")
async def health_check():
    return {"status": "running"}

@app.post("/webhook")
async def handle_whatsapp_webhook(request: Request):
    body = await request.json()
    
    # 过滤非用户发来的文本（如回执、在线状态等）
    if body.get("typeWebhook") != "incomingMessageReceived":
        return {"status": "ignored"}
        
    msg_data = body.get("messageData", {})
    if msg_data.get("typeMessage") != "textMessage":
        return {"status": "ignored"}
        
    chat_id = body.get("senderData", {}).get("chatId")
    text = msg_data.get("textMessageData", {}).get("textMessage", "").strip()
    
    if not chat_id or not text:
        return {"status": "empty"}

    # 如果该用户正在倒计时中，取消旧计时，把新内容拼进去
    if chat_id in user_buffers:
        user_buffers[chat_id]["task"].cancel()
        user_buffers[chat_id]["messages"].append(text)
    else:
        user_buffers[chat_id] = {
            "messages": [text],
            "task": None
        }
    
    # 启动全新的 30 秒倒计时
    user_buffers[chat_id]["task"] = asyncio.create_task(delay_and_send(chat_id))
    return {"status": "buffering"}
