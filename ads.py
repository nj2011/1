import requests
import sqlite3
import json
import threading
import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from dataclasses import dataclass
from flask import Flask, request, jsonify

# ============ SETUP ============

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ad_bot.log'),
        logging.StreamHandler()
    ]
)

# Flask app for webhook
app = Flask(__name__)

# Global bot instance
bot_instance = None

# ============ ENUMS ============

class AdStatus(Enum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    POSTED = "posted"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ACTIVE = "active"
    PAUSED = "paused"
    DELETED = "deleted"
    PINNED = "pinned"

class AdType(Enum):
    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    DOCUMENT = "document"
    FORWARD = "forward"

# ============ DATABASE PATH ============

def get_db_path():
    """Get database path - works on Render and locally"""
    if 'RENDER' in os.environ:
        data_dir = '/data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        return os.path.join(data_dir, 'ad_bot.db')
    else:
        return 'ad_bot.db'

# ============ ACCURATE TIMER CLASS ============

class AccurateTimer:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.running = True
        self.timer_thread = None
        
    def start_timer_processor(self):
        self.timer_thread = threading.Thread(target=self._process_timers, daemon=True)
        self.timer_thread.start()
        logging.info("Timer processor started")
    
    def _process_timers(self):
        while self.running:
            try:
                now = datetime.now()
                conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT t.*, a.source_chat_id, a.source_message_id, a.pin, a.pin_duration, a.delete_after
                    FROM timers t
                    JOIN ads a ON t.ad_id = a.id
                    WHERE t.is_active = 1 AND t.next_run <= ?
                    AND (t.max_runs IS NULL OR t.total_runs < t.max_runs)
                ''', (now.isoformat(),))
                
                due_timers = cursor.fetchall()
                conn.close()
                
                for timer in due_timers:
                    self._execute_timer(timer)
                time.sleep(1)
            except Exception as e:
                logging.error(f"Timer error: {e}")
                time.sleep(5)
    
    def _execute_timer(self, timer):
        try:
            timer_id = timer[0]
            ad_id = timer[1]
            interval_value = timer[2]
            interval_unit = timer[3]
            total_runs = timer[6] if len(timer) > 6 else 0
            max_runs = timer[7] if len(timer) > 7 else None
            channels_json = timer[10] if len(timer) > 10 else None
            source_chat_id = timer[13] if len(timer) > 13 else None
            source_message_id = timer[14] if len(timer) > 14 else None
            pin = timer[15] if len(timer) > 15 else False
            pin_duration = timer[16] if len(timer) > 16 else 0
            delete_after = timer[17] if len(timer) > 17 else 0
            
            channels = json.loads(channels_json) if channels_json else None
            if not channels:
                channels = [self.bot.channel_manager.get_default_channel()]
            
            for channel in channels:
                if channel:
                    result = self.bot.forward_message_to_channel_with_result(
                        channel, source_chat_id, source_message_id
                    )
                    if result:
                        self.bot.channel_manager.update_post_count(channel)
                        self.bot.record_posted_message(result, channel, ad_id, pin, pin_duration, delete_after)
                        if pin:
                            self.bot.pin_message(channel, result, pin_duration)
                        logging.info(f"Timer {timer_id} posted to {channel}")
            
            update_conn = sqlite3.connect(get_db_path(), check_same_thread=False)
            update_cursor = update_conn.cursor()
            new_total = (total_runs or 0) + 1
            is_active_timer = 0 if max_runs and new_total >= max_runs else 1
            
            update_cursor.execute('UPDATE timers SET last_run = ?, total_runs = ?, is_active = ? WHERE id = ?',
                                  (datetime.now().isoformat(), new_total, is_active_timer, timer_id))
            update_conn.commit()
            update_conn.close()
            
            if is_active_timer:
                self._update_timer_schedule(timer_id)
        except Exception as e:
            logging.error(f"Execute timer error: {e}")
    
    def _update_timer_schedule(self, timer_id):
        try:
            conn = sqlite3.connect(get_db_path(), check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('SELECT interval_value, interval_unit, next_run FROM timers WHERE id = ?', (timer_id,))
            result = cursor.fetchone()
            if not result:
                conn.close()
                return
            
            interval_value, interval_unit, last_next_run = result
            last_run_time = datetime.fromisoformat(last_next_run) if isinstance(last_next_run, str) else last_next_run
            
            if interval_unit == 'minutes':
                next_time = last_run_time + timedelta(minutes=interval_value)
            elif interval_unit == 'hours':
                next_time = last_run_time + timedelta(hours=interval_value)
            else:
                next_time = last_run_time + timedelta(days=interval_value)
            
            cursor.execute('UPDATE timers SET next_run = ? WHERE id = ?', (next_time.isoformat(), timer_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logging.error(f"Update schedule error: {e}")

# ============ CHANNEL MANAGER ============

class ChannelManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_channel_table()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    def init_channel_table(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT UNIQUE,
                name TEXT,
                is_active BOOLEAN DEFAULT 1,
                is_default BOOLEAN DEFAULT 0,
                created_at TEXT,
                description TEXT,
                post_count INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('SELECT COUNT(*) FROM channels')
        if cursor.fetchone()[0] == 0:
            cursor.execute('INSERT INTO channels (channel_id, name, is_default, created_at) VALUES (?, ?, ?, ?)',
                          ('@default', 'Default Channel', 1, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    
    def add_channel(self, channel_id: str, name: str = None) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT INTO channels (channel_id, name, created_at) VALUES (?, ?, ?)',
                          (channel_id, name or channel_id, datetime.now().isoformat()))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Add channel error: {e}")
            return False
    
    def remove_channel(self, channel_id: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT is_default FROM channels WHERE channel_id = ?', (channel_id,))
            result = cursor.fetchone()
            if result and result[0] == 1:
                conn.close()
                return False
            cursor.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Remove channel error: {e}")
            return False
    
    def set_default_channel(self, channel_id: str) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE channels SET is_default = 0')
            cursor.execute('UPDATE channels SET is_default = 1, is_active = 1 WHERE channel_id = ?', (channel_id,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Set default error: {e}")
            return False
    
    def toggle_channel(self, channel_id: str, active: bool) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE channels SET is_active = ? WHERE channel_id = ?', (1 if active else 0, channel_id))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Toggle channel error: {e}")
            return False
    
    def get_all_channels(self, only_active: bool = True) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = 'SELECT channel_id, name, is_active, is_default, post_count FROM channels'
        if only_active:
            query += ' WHERE is_active = 1'
        query += ' ORDER BY is_default DESC, name ASC'
        cursor.execute(query)
        channels = []
        for row in cursor.fetchall():
            channels.append({
                'channel_id': row[0],
                'name': row[1],
                'is_active': bool(row[2]),
                'is_default': bool(row[3]),
                'post_count': row[4]
            })
        conn.close()
        return channels
    
    def get_default_channel(self) -> str:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT channel_id FROM channels WHERE is_default = 1 AND is_active = 1')
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    
    def update_post_count(self, channel_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE channels SET post_count = post_count + 1 WHERE channel_id = ?', (channel_id,))
        conn.commit()
        conn.close()

# ============ SCHEDULE MANAGER ============

class ScheduleManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.running = True
        self.scheduler_thread = None
        
    def start_scheduler(self):
        self.scheduler_thread = threading.Thread(target=self._process_scheduled_tasks, daemon=True)
        self.scheduler_thread.start()
        logging.info("Schedule manager started")
    
    def _process_scheduled_tasks(self):
        while self.running:
            try:
                now = datetime.now()
                
                conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, message_id, channel_id FROM posted_messages
                    WHERE scheduled_delete_at IS NOT NULL AND scheduled_delete_at <= ? AND is_deleted = 0
                ''', (now.isoformat(),))
                to_delete = cursor.fetchall()
                conn.close()
                
                for record in to_delete:
                    self._execute_delete(record)
                
                conn2 = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor2 = conn2.cursor()
                cursor2.execute('''
                    SELECT id, message_id, channel_id FROM posted_messages
                    WHERE scheduled_unpin_at IS NOT NULL AND scheduled_unpin_at <= ? AND is_pinned = 1
                ''', (now.isoformat(),))
                to_unpin = cursor2.fetchall()
                conn2.close()
                
                for record in to_unpin:
                    self._execute_unpin(record)
                
                time.sleep(30)
            except Exception as e:
                logging.error(f"Scheduler error: {e}")
                time.sleep(60)
    
    def _execute_delete(self, record):
        try:
            record_id, message_id, channel_id = record
            if message_id and channel_id and self.bot.delete_message(channel_id, message_id):
                conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?',
                              (datetime.now().isoformat(), record_id))
                conn.commit()
                conn.close()
        except Exception as e:
            logging.error(f"Delete error: {e}")
    
    def _execute_unpin(self, record):
        try:
            record_id, message_id, channel_id = record
            if message_id and channel_id and self.bot.unpin_message(channel_id, message_id):
                conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute('UPDATE posted_messages SET is_pinned = 0, unpinned_at = ? WHERE id = ?',
                              (datetime.now().isoformat(), record_id))
                conn.commit()
                conn.close()
        except Exception as e:
            logging.error(f"Unpin error: {e}")

# ============ MAIN BOT CLASS ============

class ForwardAdBot:
    def __init__(self, bot_token: str, admin_id: int):
        self.bot_token = bot_token
        self.admin_id = admin_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.running = True
        self.pending_requests = {}
        self.db_path = get_db_path()
        self.init_database()
        self.channel_manager = ChannelManager(self.db_path)
        self.timer_manager = AccurateTimer(self)
        self.schedule_manager = ScheduleManager(self)
        
    def init_database(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT,
                ad_type TEXT,
                status TEXT,
                created_at TEXT,
                source_message_id INTEGER,
                source_chat_id TEXT,
                timer_id INTEGER,
                media_type TEXT,
                pin BOOLEAN DEFAULT 0,
                pin_duration INTEGER DEFAULT 0,
                delete_after INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posted_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                channel_id TEXT,
                ad_id INTEGER,
                posted_at TEXT,
                scheduled_delete_at TEXT,
                scheduled_unpin_at TEXT,
                is_pinned BOOLEAN DEFAULT 0,
                is_deleted BOOLEAN DEFAULT 0,
                deleted_at TEXT,
                unpinned_at TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS timers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ad_id INTEGER,
                interval_value INTEGER,
                interval_unit TEXT,
                next_run TEXT,
                last_run TEXT,
                total_runs INTEGER DEFAULT 0,
                max_runs INTEGER,
                is_active BOOLEAN DEFAULT 1,
                created_at TEXT,
                channel_ids TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                date TEXT PRIMARY KEY,
                ads_posted INTEGER DEFAULT 0,
                ads_received INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_posted_messages_delete ON posted_messages(scheduled_delete_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_posted_messages_unpin ON posted_messages(scheduled_unpin_at)')
        
        conn.commit()
        conn.close()
        logging.info(f"Database initialized at {self.db_path}")

    # ============ TELEGRAM API METHODS ============
    
    def api_request(self, method: str, payload: Dict = None, max_retries: int = 3) -> Optional[Dict]:
        url = f"{self.base_url}/{method}"
        for attempt in range(max_retries):
            try:
                if method in ["sendMessage", "forwardMessage", "pinChatMessage", "unpinChatMessage", "deleteMessage"]:
                    response = requests.post(url, json=payload, timeout=30)
                else:
                    response = requests.get(url, params=payload, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    time.sleep(retry_after)
            except Exception as e:
                logging.error(f"API request error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
        return None
    
    def forward_message_to_channel_with_result(self, channel_id: str, from_chat_id: str, message_id: int) -> Optional[int]:
        if not from_chat_id or not message_id:
            return None
        payload = {"chat_id": channel_id, "from_chat_id": from_chat_id, "message_id": message_id}
        result = self.api_request("forwardMessage", payload)
        if result and result.get('ok'):
            return result.get('result', {}).get('message_id')
        return None
    
    def pin_message(self, channel_id: str, message_id: int, duration: int = 0) -> bool:
        payload = {"chat_id": channel_id, "message_id": message_id, "disable_notification": True}
        result = self.api_request("pinChatMessage", payload)
        return result and result.get('ok')
    
    def unpin_message(self, channel_id: str, message_id: int = None) -> bool:
        payload = {"chat_id": channel_id}
        if message_id:
            payload["message_id"] = message_id
        result = self.api_request("unpinChatMessage", payload)
        return result and result.get('ok')
    
    def delete_message(self, channel_id: str, message_id: int) -> bool:
        payload = {"chat_id": channel_id, "message_id": message_id}
        result = self.api_request("deleteMessage", payload)
        return result and result.get('ok')
    
    def send_message(self, chat_id: int, text: str, keyboard: Dict = None):
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if keyboard:
            payload["reply_markup"] = keyboard
        self.api_request("sendMessage", payload)
    
    def answer_callback(self, callback_id: str, text: str = None, show_alert: bool = False):
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
            payload["show_alert"] = show_alert
        self.api_request("answerCallbackQuery", payload)
    
    def set_webhook(self, webhook_url: str):
        result = self.api_request("setWebhook", {"url": webhook_url})
        return result and result.get('ok')
    
    def delete_webhook(self):
        result = self.api_request("deleteWebhook")
        return result and result.get('ok')
    
    # ============ MESSAGE PARSING ============
    
    def extract_message_data(self, message: Dict) -> Dict:
        data = {
            'source_chat_id': str(message.get('chat', {}).get('id')),
            'source_message_id': message.get('message_id'),
            'text': message.get('text', ''),
            'caption': message.get('caption', ''),
            'media_type': None,
        }
        if 'photo' in message:
            data['media_type'] = 'photo'
            data['text'] = data['caption']
        elif 'video' in message:
            data['media_type'] = 'video'
            data['text'] = data['caption']
        return data
    
    # ============ SCHEDULING METHODS ============
    
    def schedule_delete(self, channel_id: str, message_id: int, ad_id: int, delete_after_seconds: int):
        if delete_after_seconds <= 0:
            return
        delete_time = datetime.now() + timedelta(seconds=delete_after_seconds)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('UPDATE posted_messages SET scheduled_delete_at = ? WHERE channel_id = ? AND message_id = ?',
                      (delete_time.isoformat(), channel_id, message_id))
        conn.commit()
        conn.close()
    
    def schedule_unpin(self, channel_id: str, message_id: int, ad_id: int, unpin_after_seconds: int):
        if unpin_after_seconds <= 0:
            return
        unpin_time = datetime.now() + timedelta(seconds=unpin_after_seconds)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('UPDATE posted_messages SET scheduled_unpin_at = ? WHERE channel_id = ? AND message_id = ?',
                      (unpin_time.isoformat(), channel_id, message_id))
        conn.commit()
        conn.close()
    
    def record_posted_message(self, message_id: int, channel_id: str, ad_id: int, pin: bool = False, 
                             pin_duration: int = 0, delete_after: int = 0) -> int:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO posted_messages (message_id, channel_id, ad_id, posted_at, is_pinned) VALUES (?, ?, ?, ?, ?)',
                      (message_id, channel_id, ad_id, datetime.now().isoformat(), 1 if pin else 0))
        record_id = cursor.lastrowid
        conn.commit()
        conn.close()
        if pin and pin_duration > 0:
            self.schedule_unpin(channel_id, message_id, ad_id, pin_duration)
        if delete_after > 0:
            self.schedule_delete(channel_id, message_id, ad_id, delete_after)
        return record_id
    
    # ============ DELETE POSTS MANAGER ============
    
    def show_delete_posts_menu(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        active_posts = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 1')
        deleted_posts = cursor.fetchone()[0]
        conn.close()
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "Delete Single Post", "callback_data": "delete_single"}],
                [{"text": "Delete by Channel", "callback_data": "delete_by_channel"}],
                [{"text": "Delete by Date Range", "callback_data": "delete_by_date"}],
                [{"text": "Delete ALL Posts", "callback_data": "delete_all_confirm"}],
                [{"text": "List Recent Posts", "callback_data": "list_recent_posts"}],
                [{"text": "View Deleted Posts", "callback_data": "view_deleted_posts"}],
                [{"text": "Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        
        text = f"DELETE POSTS MANAGER\n\nActive Posts: {active_posts}\nDeleted Posts: {deleted_posts}\n\nSelect an option:"
        self.send_message(chat_id, text, keyboard)
    
    def list_recent_posts_handler(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, message_id, channel_id, posted_at, is_pinned, is_deleted FROM posted_messages ORDER BY posted_at DESC LIMIT 30')
        posts = cursor.fetchall()
        conn.close()
        
        if not posts:
            self.send_message(chat_id, "No posts found.")
            return
        
        keyboard = {"inline_keyboard": []}
        for post in posts:
            post_id, msg_id, channel_id, posted_at, is_pinned, is_deleted = post
            status = "PIN" if is_pinned else ("DEL" if is_deleted else "ACT")
            keyboard["inline_keyboard"].append([
                {"text": f"{status} {channel_id[:20]} - {posted_at[:16]}", "callback_data": f"select_post_{post_id}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "delete_menu"}])
        self.send_message(chat_id, "Select a post to manage:", keyboard)
    
    def show_post_delete_options(self, chat_id: int, post_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT message_id, channel_id, posted_at, is_pinned, is_deleted FROM posted_messages WHERE id = ?', (post_id,))
        post = cursor.fetchone()
        conn.close()
        
        if not post:
            self.send_message(chat_id, "Post not found")
            return
        
        message_id, channel_id, posted_at, is_pinned, is_deleted = post
        if is_deleted:
            self.send_message(chat_id, f"Post #{post_id} is already deleted.")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "DELETE THIS POST", "callback_data": f"confirm_delete_post_{post_id}"}],
                [{"text": "Toggle Pin Status", "callback_data": f"toggle_pin_post_{post_id}"}],
                [{"text": "Back", "callback_data": "list_recent_posts"}]
            ]
        }
        text = f"Post to Delete:\n\nID: {post_id}\nChannel: {channel_id}\nPosted: {posted_at}\nStatus: {'Pinned' if is_pinned else 'Not pinned'}\n\nAre you sure?"
        self.send_message(chat_id, text, keyboard)
    
    def confirm_delete_post(self, chat_id: int, post_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT message_id, channel_id FROM posted_messages WHERE id = ? AND is_deleted = 0', (post_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            self.send_message(chat_id, "Post not found")
            return
        
        message_id, channel_id = result
        if self.delete_message(channel_id, message_id):
            cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?',
                          (datetime.now().isoformat(), post_id))
            conn.commit()
            self.send_message(chat_id, f"Post #{post_id} deleted!")
        else:
            self.send_message(chat_id, "Failed to delete post")
        conn.close()
        self.list_recent_posts_handler(chat_id)
    
    def toggle_post_pin(self, chat_id: int, post_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT message_id, channel_id, is_pinned FROM posted_messages WHERE id = ?', (post_id,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            self.send_message(chat_id, "Post not found")
            return
        
        message_id, channel_id, is_pinned = result
        if is_pinned:
            if self.unpin_message(channel_id, message_id):
                cursor.execute('UPDATE posted_messages SET is_pinned = 0, unpinned_at = ? WHERE id = ?',
                              (datetime.now().isoformat(), post_id))
                conn.commit()
                self.send_message(chat_id, f"Unpinned post #{post_id}")
        else:
            if self.pin_message(channel_id, message_id):
                cursor.execute('UPDATE posted_messages SET is_pinned = 1 WHERE id = ?', (post_id,))
                conn.commit()
                self.send_message(chat_id, f"Pinned post #{post_id}")
        conn.close()
        self.list_recent_posts_handler(chat_id)
    
    def show_delete_by_channel(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT channel_id, COUNT(*) FROM posted_messages WHERE is_deleted = 0 GROUP BY channel_id')
        channels = cursor.fetchall()
        conn.close()
        
        if not channels:
            self.send_message(chat_id, "No posts found.")
            return
        
        keyboard = {"inline_keyboard": []}
        for ch, count in channels:
            keyboard["inline_keyboard"].append([
                {"text": f"Delete {ch[:30]} ({count} posts)", "callback_data": f"delete_channel_{ch}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "delete_menu"}])
        self.send_message(chat_id, "Select channel to delete posts from:", keyboard)
    
    def execute_delete_channel_posts(self, chat_id: int, channel_id: str):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, message_id FROM posted_messages WHERE channel_id = ? AND is_deleted = 0', (channel_id,))
        posts = cursor.fetchall()
        
        deleted = 0
        for post_id, message_id in posts:
            if self.delete_message(channel_id, message_id):
                cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?',
                              (datetime.now().isoformat(), post_id))
                deleted += 1
        conn.commit()
        conn.close()
        self.send_message(chat_id, f"Deleted {deleted} posts from {channel_id}")
        self.show_delete_posts_menu(chat_id)
    
    def show_delete_by_date(self, chat_id: int):
        keyboard = {
            "inline_keyboard": [
                [{"text": "Last 24 Hours", "callback_data": "delete_date_24h"}],
                [{"text": "Last 7 Days", "callback_data": "delete_date_7d"}],
                [{"text": "Last 30 Days", "callback_data": "delete_date_30d"}],
                [{"text": "Back", "callback_data": "delete_menu"}]
            ]
        }
        self.send_message(chat_id, "Select date range:", keyboard)
    
    def execute_delete_by_date(self, chat_id: int, days: int):
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, message_id, channel_id FROM posted_messages WHERE posted_at <= ? AND is_deleted = 0', (cutoff,))
        posts = cursor.fetchall()
        
        if not posts:
            conn.close()
            self.send_message(chat_id, f"No posts older than {days} days")
            return
        
        deleted = 0
        for post_id, message_id, channel_id in posts:
            if self.delete_message(channel_id, message_id):
                cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?',
                              (datetime.now().isoformat(), post_id))
                deleted += 1
        conn.commit()
        conn.close()
        self.send_message(chat_id, f"Deleted {deleted} posts older than {days} days")
        self.show_delete_posts_menu(chat_id)
    
    def show_delete_all_confirm(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        total = cursor.fetchone()[0]
        conn.close()
        
        if total == 0:
            self.send_message(chat_id, "No posts to delete.")
            return
        
        self.pending_requests[chat_id] = {'awaiting_delete_all_confirm': True}
        self.send_message(chat_id, f"Type CONFIRM to delete ALL {total} posts. This cannot be undone!")
    
    def view_deleted_posts(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, message_id, channel_id, posted_at, deleted_at FROM posted_messages WHERE is_deleted = 1 ORDER BY deleted_at DESC LIMIT 20')
        posts = cursor.fetchall()
        conn.close()
        
        if not posts:
            self.send_message(chat_id, "No deleted posts found.")
            return
        
        text = "DELETED POSTS HISTORY\n\n"
        for post in posts:
            post_id, msg_id, channel_id, posted_at, deleted_at = post
            text += f"#{post_id} - {channel_id[:20]}\n   Posted: {posted_at[:16]}\n   Deleted: {deleted_at[:16]}\n\n"
        self.send_message(chat_id, text)
    
    # ============ CHANNEL MANAGEMENT ============
    
    def show_channel_menu(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=False)
        keyboard = {
            "inline_keyboard": [
                [{"text": "Add Channel", "callback_data": "ch_add"}],
                [{"text": "List Channels", "callback_data": "ch_list"}],
                [{"text": "Set Default", "callback_data": "ch_default"}],
                [{"text": "Toggle Active", "callback_data": "ch_toggle"}],
                [{"text": "Remove Channel", "callback_data": "ch_remove"}],
                [{"text": "Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        text = f"CHANNEL MANAGEMENT\n\nTotal Channels: {len(channels)}\nActive: {len([c for c in channels if c['is_active']])}"
        self.send_message(chat_id, text, keyboard)
    
    def list_channels_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=False)
        if not channels:
            self.send_message(chat_id, "No channels added.")
            return
        text = "YOUR CHANNELS\n\n"
        for ch in channels:
            star = "⭐" if ch['is_default'] else "📢"
            status = "✅" if ch['is_active'] else "❌"
            text += f"{star} {ch['name']}\nID: {ch['channel_id']}\nStatus: {status}\nPosts: {ch['post_count']}\n\n"
        self.send_message(chat_id, text)
    
    # ============ TIMER HANDLERS ============
    
    def show_timer_menu(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active_timers = cursor.fetchone()[0]
        conn.close()
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "Create Timer", "callback_data": "timer_create"}],
                [{"text": "List Timers", "callback_data": "timer_list"}],
                [{"text": "Pause Timer", "callback_data": "timer_pause"}],
                [{"text": "Delete Timer", "callback_data": "timer_delete"}],
                [{"text": "Back", "callback_data": "back_to_main"}]
            ]
        }
        text = f"TIMER MANAGEMENT\n\nActive Timers: {active_timers}"
        self.send_message(chat_id, text, keyboard)
    
    def show_timer_interval_menu(self, chat_id: int):
        keyboard = {
            "inline_keyboard": [
                [{"text": "1 minute", "callback_data": "timer_interval_1_minutes"}],
                [{"text": "5 minutes", "callback_data": "timer_interval_5_minutes"}],
                [{"text": "15 minutes", "callback_data": "timer_interval_15_minutes"}],
                [{"text": "30 minutes", "callback_data": "timer_interval_30_minutes"}],
                [{"text": "1 hour", "callback_data": "timer_interval_1_hours"}],
                [{"text": "2 hours", "callback_data": "timer_interval_2_hours"}],
                [{"text": "4 hours", "callback_data": "timer_interval_4_hours"}],
                [{"text": "6 hours", "callback_data": "timer_interval_6_hours"}],
                [{"text": "12 hours", "callback_data": "timer_interval_12_hours"}],
                [{"text": "1 day", "callback_data": "timer_interval_1_days"}],
                [{"text": "Back", "callback_data": "timer_menu"}]
            ]
        }
        self.send_message(chat_id, "Select interval:", keyboard)
    
    def list_timers_handler(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, interval_value, interval_unit, next_run, total_runs, max_runs FROM timers WHERE is_active = 1 ORDER BY next_run ASC')
        timers = cursor.fetchall()
        conn.close()
        
        if not timers:
            self.send_message(chat_id, "No active timers.")
            return
        
        text = "ACTIVE TIMERS\n\n"
        for timer in timers:
            timer_id, interval_value, interval_unit, next_run_str, total_runs, max_runs = timer
            next_run = datetime.fromisoformat(next_run_str)
            remaining = next_run - datetime.now()
            minutes = remaining.seconds // 60
            text += f"#{timer_id}: Every {interval_value} {interval_unit}\n   Next in {minutes}m, Runs: {total_runs}/{max_runs or 'unlimited'}\n\n"
        self.send_message(chat_id, text)
    
    # ============ FORWARD HANDLERS ============
    
    def handle_forwarded_message(self, message: Dict, chat_id: int, user_id: int):
        if user_id != self.admin_id:
            self.send_message(chat_id, "Unauthorized!")
            return
        
        msg_data = self.extract_message_data(message)
        self.pending_requests[chat_id] = msg_data
        
        # Update stats
        today = datetime.now().date().isoformat()
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO stats (date, ads_received) VALUES (?, COALESCE((SELECT ads_received FROM stats WHERE date = ?), 0) + 1)', (today, today))
        conn.commit()
        conn.close()
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "Post Now", "callback_data": "forward_now"}],
                [{"text": "Post & Pin", "callback_data": "forward_pin"}],
                [{"text": "Auto-Timer", "callback_data": "forward_timer"}],
                [{"text": "Advanced", "callback_data": "forward_advanced"}],
                [{"text": "Cancel", "callback_data": "cancel"}]
            ]
        }
        
        preview = msg_data['text'][:100] if msg_data['text'] else f"Media message"
        text = f"MESSAGE RECEIVED!\n\nPreview: {preview}\n\nWhat would you like to do?"
        self.send_message(chat_id, text, keyboard)
    
    def execute_forward_enhanced(self, chat_id: int, channel_id: str = None, forward_all: bool = False, 
                                 pin: bool = False, pin_duration: int = 0, delete_after: int = 0):
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "No message found.")
            return
        
        pending = self.pending_requests[chat_id]
        
        if forward_all:
            channels = self.channel_manager.get_all_channels(only_active=True)
            channel_ids = [ch['channel_id'] for ch in channels]
        else:
            channel_ids = [channel_id] if channel_id else [self.channel_manager.get_default_channel()]
        
        success_count = 0
        for ch_id in channel_ids:
            new_msg_id = self.forward_message_to_channel_with_result(ch_id, pending['source_chat_id'], pending['source_message_id'])
            if new_msg_id:
                success_count += 1
                self.channel_manager.update_post_count(ch_id)
                self.record_posted_message(new_msg_id, ch_id, 0, pin, pin_duration, delete_after)
                if pin:
                    self.pin_message(ch_id, new_msg_id, pin_duration)
        
        # Update stats
        today = datetime.now().date().isoformat()
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('UPDATE stats SET ads_posted = ads_posted + ? WHERE date = ?', (success_count, today))
        conn.commit()
        conn.close()
        
        result_msg = f"Posted to {success_count}/{len(channel_ids)} channels!"
        if pin:
            result_msg += f"\nPinned for {self._format_duration(pin_duration)}"
        if delete_after > 0:
            result_msg += f"\nDeletes after {self._format_duration(delete_after)}"
        
        self.send_message(chat_id, result_msg)
        self.pending_requests.pop(chat_id, None)
    
    def show_forward_advanced(self, chat_id: int):
        keyboard = {
            "inline_keyboard": [
                [{"text": "Custom Pin Duration", "callback_data": "forward_custom_pin"}],
                [{"text": "Auto-Delete", "callback_data": "forward_auto_delete"}],
                [{"text": "Pin & Auto-Delete", "callback_data": "forward_pin_delete"}],
                [{"text": "Select Channels", "callback_data": "forward_select_channels"}],
                [{"text": "Back", "callback_data": "cancel"}]
            ]
        }
        self.send_message(chat_id, "ADVANCED OPTIONS", keyboard)
    
    def show_pin_duration_menu(self, chat_id: int, action: str):
        self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
        self.pending_requests[chat_id]['awaiting_pin_duration'] = action
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "1 Hour", "callback_data": "pin_dur_3600"}],
                [{"text": "3 Hours", "callback_data": "pin_dur_10800"}],
                [{"text": "6 Hours", "callback_data": "pin_dur_21600"}],
                [{"text": "12 Hours", "callback_data": "pin_dur_43200"}],
                [{"text": "1 Day", "callback_data": "pin_dur_86400"}],
                [{"text": "3 Days", "callback_data": "pin_dur_259200"}],
                [{"text": "1 Week", "callback_data": "pin_dur_604800"}],
                [{"text": "Forever", "callback_data": "pin_dur_0"}],
                [{"text": "Cancel", "callback_data": "cancel"}]
            ]
        }
        self.send_message(chat_id, "Select pin duration:", keyboard)
    
    def show_delete_after_menu(self, chat_id: int):
        self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
        self.pending_requests[chat_id]['awaiting_delete_after'] = True
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "1 Hour", "callback_data": "del_after_3600"}],
                [{"text": "3 Hours", "callback_data": "del_after_10800"}],
                [{"text": "6 Hours", "callback_data": "del_after_21600"}],
                [{"text": "12 Hours", "callback_data": "del_after_43200"}],
                [{"text": "1 Day", "callback_data": "del_after_86400"}],
                [{"text": "3 Days", "callback_data": "del_after_259200"}],
                [{"text": "Never", "callback_data": "del_after_0"}],
                [{"text": "Cancel", "callback_data": "cancel"}]
            ]
        }
        self.send_message(chat_id, "Select auto-delete time:", keyboard)
    
    def forward_now_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=True)
        if not channels:
            self.send_message(chat_id, "No active channels!")
            return
        
        keyboard = {"inline_keyboard": []}
        for ch in channels:
            keyboard["inline_keyboard"].append([
                {"text": f"{ch['name']}", "callback_data": f"forward_ch_{ch['channel_id']}"}
            ])
        keyboard["inline_keyboard"].append([
            {"text": "Post to ALL", "callback_data": "forward_all"},
            {"text": "Cancel", "callback_data": "cancel"}
        ])
        self.send_message(chat_id, "Select channel:", keyboard)
    
    def forward_to_single_channel(self, chat_id: int, channel_id: str):
        self.execute_forward_enhanced(chat_id, channel_id=channel_id)
    
    # ============ STATISTICS AND HELP ============
    
    def show_stats(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        today = datetime.now().date().isoformat()
        cursor.execute('SELECT ads_posted, ads_received FROM stats WHERE date = ?', (today,))
        today_stats = cursor.fetchone()
        cursor.execute('SELECT SUM(ads_posted), SUM(ads_received) FROM stats')
        total_stats = cursor.fetchone()
        cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        active_posts = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active_timers = cursor.fetchone()[0]
        conn.close()
        
        channels = self.channel_manager.get_all_channels(only_active=False)
        
        text = f"STATISTICS\n\nToday:\nReceived: {today_stats[1] if today_stats else 0}\nPosted: {today_stats[0] if today_stats else 0}\n\nAll Time:\nReceived: {total_stats[1] if total_stats else 0}\nPosted: {total_stats[0] if total_stats else 0}\n\nCurrent:\nChannels: {len(channels)}\nTimers: {active_timers}\nActive Posts: {active_posts}"
        self.send_message(chat_id, text)
    
    def show_help(self, chat_id: int):
        text = """HELP\n\nCommands:\n/start - Main menu\n/stats - Statistics\n/help - This help\n\nHow to use:\n1. Add channel with /channels\n2. Forward a message to the bot\n3. Choose Post Now or Post & Pin\n4. Select channel(s)\n\nFeatures:\n- Pin messages for set durations\n- Auto-delete after set time\n- Auto-timers for recurring posts\n- Delete posts manager\n\nRequirements:\nBot must be admin in channel with permissions to post, pin, and delete messages."""
        self.send_message(chat_id, text)
    
    def show_main_menu(self, chat_id: int):
        keyboard = {
            "inline_keyboard": [
                [{"text": "Post Message", "callback_data": "forward_prompt"}],
                [{"text": "Auto-Timers", "callback_data": "timer_menu"}],
                [{"text": "Manage Channels", "callback_data": "channel_menu"}],
                [{"text": "Delete Posts", "callback_data": "delete_menu"}],
                [{"text": "Statistics", "callback_data": "stats_main"}],
                [{"text": "Help", "callback_data": "help_main"}]
            ]
        }
        
        default_ch = self.channel_manager.get_default_channel() or 'Not set'
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        timers = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        posts = cursor.fetchone()[0]
        conn.close()
        
        text = f"AD BOT\n\nDefault channel: {default_ch}\nActive timers: {timers}\nActive posts: {posts}\n\nForward any message to get started!"
        self.send_message(chat_id, text, keyboard)
    
    def _format_duration(self, seconds: int) -> str:
        if seconds <= 0:
            return "Forever"
        hours = seconds // 3600
        days = hours // 24
        if days > 0:
            return f"{days}d"
        return f"{hours}h"
    
    # ============ TEXT INPUT HANDLER ============
    
    def handle_text_input(self, chat_id: int, text: str, user_id: int):
        if user_id != self.admin_id:
            return
        
        # Handle add channel
        if chat_id in self.pending_requests and self.pending_requests[chat_id].get('action') == 'add_channel':
            parts = text.split(',', 1)
            channel_id = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else channel_id
            
            if self.channel_manager.add_channel(channel_id, name):
                self.send_message(chat_id, f"Channel '{name}' added!")
            else:
                self.send_message(chat_id, "Failed to add channel")
            
            self.pending_requests.pop(chat_id, None)
            self.show_channel_menu(chat_id)
            return
        
        # Handle delete all confirmation
        if chat_id in self.pending_requests and self.pending_requests[chat_id].get('awaiting_delete_all_confirm'):
            if text.strip().upper() == "CONFIRM":
                conn = sqlite3.connect(self.db_path, check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute('SELECT id, message_id, channel_id FROM posted_messages WHERE is_deleted = 0')
                posts = cursor.fetchall()
                deleted = 0
                for post_id, message_id, channel_id in posts:
                    if self.delete_message(channel_id, message_id):
                        cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?',
                                      (datetime.now().isoformat(), post_id))
                        deleted += 1
                conn.commit()
                conn.close()
                self.send_message(chat_id, f"Deleted {deleted} posts!")
            else:
                self.send_message(chat_id, "Cancelled")
            self.pending_requests.pop(chat_id, None)
            self.show_delete_posts_menu(chat_id)
            return
        
        # Handle commands
        if text.startswith('/'):
            cmd = text.split()[0].lower()
            if cmd == '/start':
                self.show_main_menu(chat_id)
            elif cmd == '/stats':
                self.show_stats(chat_id)
            elif cmd == '/help':
                self.show_help(chat_id)
            elif cmd == '/channels':
                self.show_channel_menu(chat_id)
    
    # ============ CALLBACK HANDLER ============
    
    def handle_callback(self, callback_data: str, chat_id: int, message_id: int, user_id: int, callback_id: str = None):
        if user_id != self.admin_id:
            if callback_id:
                self.answer_callback(callback_id, "Unauthorized!", True)
            return
        
        # Main menu
        if callback_data == "forward_prompt":
            self.send_message(chat_id, "Forward any message to me!")
        elif callback_data == "timer_menu":
            self.show_timer_menu(chat_id)
        elif callback_data == "channel_menu":
            self.show_channel_menu(chat_id)
        elif callback_data == "delete_menu":
            self.show_delete_posts_menu(chat_id)
        elif callback_data == "stats_main":
            self.show_stats(chat_id)
        elif callback_data == "help_main":
            self.show_help(chat_id)
        elif callback_data == "back_to_main":
            self.show_main_menu(chat_id)
        
        # Timer callbacks
        elif callback_data == "timer_create":
            self.show_timer_interval_menu(chat_id)
        elif callback_data == "timer_list":
            self.list_timers_handler(chat_id)
        elif callback_data == "timer_pause":
            self.pause_timer_selection(chat_id)
        elif callback_data == "timer_delete":
            self.delete_timer_selection(chat_id)
        elif callback_data.startswith("timer_pause_"):
            timer_id = int(callback_data.replace("timer_pause_", ""))
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('UPDATE timers SET is_active = 0 WHERE id = ?', (timer_id,))
            conn.commit()
            conn.close()
            self.send_message(chat_id, f"Timer #{timer_id} paused")
        elif callback_data.startswith("timer_delete_"):
            timer_id = int(callback_data.replace("timer_delete_", ""))
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute('DELETE FROM timers WHERE id = ?', (timer_id,))
            conn.commit()
            conn.close()
            self.send_message(chat_id, f"Timer #{timer_id} deleted")
        elif callback_data.startswith("timer_interval_"):
            parts = callback_data.replace("timer_interval_", "").split("_")
            interval_value = int(parts[0])
            interval_unit = parts[1]
            if self.pending_requests.get(chat_id, {}).get('awaiting_timer'):
                self.create_timer_from_forwarded(chat_id, interval_value, interval_unit)
            else:
                self.pending_requests[chat_id] = {'awaiting_timer': True}
                self.send_message(chat_id, f"Interval set: every {interval_value} {interval_unit}\nNow forward the message!")
        
        # Forward callbacks
        elif callback_data == "forward_now":
            self.forward_now_handler(chat_id)
        elif callback_data == "forward_pin":
            self.show_pin_duration_menu(chat_id, "forward_pin")
        elif callback_data == "forward_timer":
            self.show_timer_interval_menu(chat_id)
            self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
            self.pending_requests[chat_id]['awaiting_timer'] = True
        elif callback_data == "forward_advanced":
            self.show_forward_advanced(chat_id)
        elif callback_data == "forward_custom_pin":
            self.show_pin_duration_menu(chat_id, "forward_custom_pin")
        elif callback_data == "forward_auto_delete":
            self.show_delete_after_menu(chat_id)
        elif callback_data == "forward_pin_delete":
            self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
            self.pending_requests[chat_id]['awaiting_pin_delete'] = True
            self.show_pin_duration_menu(chat_id, "forward_pin_delete")
        elif callback_data == "forward_select_channels":
            self.forward_now_handler(chat_id)
        elif callback_data == "forward_all":
            self.execute_forward_enhanced(chat_id, forward_all=True)
        elif callback_data.startswith("forward_ch_"):
            channel_id = callback_data.replace("forward_ch_", "")
            self.forward_to_single_channel(chat_id, channel_id)
        
        # Pin duration selections
        elif callback_data.startswith("pin_dur_"):
            duration = int(callback_data.replace("pin_dur_", ""))
            action = self.pending_requests.get(chat_id, {}).get('awaiting_pin_duration', 'forward_pin')
            if action == "forward_pin":
                self.execute_forward_enhanced(chat_id, forward_all=True, pin=True, pin_duration=duration)
            elif action == "forward_pin_delete":
                self.pending_requests[chat_id]['pin_duration'] = duration
                self.show_delete_after_menu(chat_id)
            elif action == "forward_custom_pin":
                self.execute_forward_enhanced(chat_id, forward_all=True, pin=True, pin_duration=duration)
            self.pending_requests.get(chat_id, {}).pop('awaiting_pin_duration', None)
        
        # Delete after selections
        elif callback_data.startswith("del_after_"):
            delete_after = int(callback_data.replace("del_after_", ""))
            if self.pending_requests.get(chat_id, {}).get('awaiting_pin_delete'):
                pin_duration = self.pending_requests[chat_id].get('pin_duration', 3600)
                self.execute_forward_enhanced(chat_id, forward_all=True, pin=True, pin_duration=pin_duration, delete_after=delete_after)
                self.pending_requests[chat_id].pop('awaiting_pin_delete', None)
            else:
                self.execute_forward_enhanced(chat_id, forward_all=True, delete_after=delete_after)
            self.pending_requests.get(chat_id, {}).pop('awaiting_delete_after', None)
        
        # Delete posts callbacks
        elif callback_data == "delete_single":
            self.list_recent_posts_handler(chat_id)
        elif callback_data == "delete_by_channel":
            self.show_delete_by_channel(chat_id)
        elif callback_data == "delete_by_date":
            self.show_delete_by_date(chat_id)
        elif callback_data == "delete_all_confirm":
            self.show_delete_all_confirm(chat_id)
        elif callback_data == "list_recent_posts":
            self.list_recent_posts_handler(chat_id)
        elif callback_data == "view_deleted_posts":
            self.view_deleted_posts(chat_id)
        elif callback_data.startswith("select_post_"):
            post_id = int(callback_data.replace("select_post_", ""))
            self.show_post_delete_options(chat_id, post_id)
        elif callback_data.startswith("confirm_delete_post_"):
            post_id = int(callback_data.replace("confirm_delete_post_", ""))
            self.confirm_delete_post(chat_id, post_id)
        elif callback_data.startswith("toggle_pin_post_"):
            post_id = int(callback_data.replace("toggle_pin_post_", ""))
            self.toggle_post_pin(chat_id, post_id)
        elif callback_data.startswith("delete_channel_"):
            channel_id = callback_data.replace("delete_channel_", "")
            self.execute_delete_channel_posts(chat_id, channel_id)
        elif callback_data.startswith("delete_date_"):
            if callback_data == "delete_date_24h":
                self.execute_delete_by_date(chat_id, 1)
            elif callback_data == "delete_date_7d":
                self.execute_delete_by_date(chat_id, 7)
            elif callback_data == "delete_date_30d":
                self.execute_delete_by_date(chat_id, 30)
        
        # Channel management callbacks
        elif callback_data == "ch_add":
            self.send_message(chat_id, "Send channel ID or @username\nOptional: Name after comma\nExample: @channel, My Channel")
            self.pending_requests[chat_id] = {'action': 'add_channel'}
        elif callback_data == "ch_list":
            self.list_channels_handler(chat_id)
        elif callback_data == "ch_default":
            self.set_default_channel_handler(chat_id)
        elif callback_data == "ch_toggle":
            self.toggle_channel_handler(chat_id)
        elif callback_data == "ch_remove":
            self.remove_channel_handler(chat_id)
        elif callback_data.startswith("set_default_"):
            channel_id = callback_data.replace("set_default_", "")
            self.channel_manager.set_default_channel(channel_id)
            self.send_message(chat_id, f"Default channel: {channel_id}")
        elif callback_data.startswith("toggle_"):
            channel_id = callback_data.replace("toggle_", "")
            channels = self.channel_manager.get_all_channels(only_active=False)
            for ch in channels:
                if ch['channel_id'] == channel_id:
                    self.channel_manager.toggle_channel(channel_id, not ch['is_active'])
                    self.send_message(chat_id, f"Channel {ch['name']} toggled")
                    break
        elif callback_data.startswith("remove_"):
            channel_id = callback_data.replace("remove_", "")
            if self.channel_manager.remove_channel(channel_id):
                self.send_message(chat_id, "Channel removed")
            else:
                self.send_message(chat_id, "Cannot remove default channel")
        
        # Cancel
        elif callback_data == "cancel":
            self.pending_requests.pop(chat_id, None)
            self.send_message(chat_id, "Cancelled")
        
        if callback_id:
            self.answer_callback(callback_id)
    
    def pause_timer_selection(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, interval_value, interval_unit FROM timers WHERE is_active = 1')
        timers = cursor.fetchall()
        conn.close()
        if not timers:
            self.send_message(chat_id, "No active timers.")
            return
        keyboard = {"inline_keyboard": []}
        for t in timers:
            keyboard["inline_keyboard"].append([
                {"text": f"Timer #{t[0]} - Every {t[1]} {t[2]}", "callback_data": f"timer_pause_{t[0]}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "timer_menu"}])
        self.send_message(chat_id, "Select timer to pause:", keyboard)
    
    def delete_timer_selection(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT id, interval_value, interval_unit FROM timers')
        timers = cursor.fetchall()
        conn.close()
        if not timers:
            self.send_message(chat_id, "No timers.")
            return
        keyboard = {"inline_keyboard": []}
        for t in timers:
            keyboard["inline_keyboard"].append([
                {"text": f"Timer #{t[0]} - Every {t[1]} {t[2]}", "callback_data": f"timer_delete_{t[0]}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "timer_menu"}])
        self.send_message(chat_id, "Select timer to delete:", keyboard)
    
    def set_default_channel_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=True)
        if not channels:
            self.send_message(chat_id, "No active channels.")
            return
        keyboard = {"inline_keyboard": []}
        for ch in channels:
            keyboard["inline_keyboard"].append([
                {"text": f"{'⭐' if ch['is_default'] else '📢'} {ch['name']}", "callback_data": f"set_default_{ch['channel_id']}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "channel_menu"}])
        self.send_message(chat_id, "Select default channel:", keyboard)
    
    def toggle_channel_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=False)
        if not channels:
            self.send_message(chat_id, "No channels.")
            return
        keyboard = {"inline_keyboard": []}
        for ch in channels:
            keyboard["inline_keyboard"].append([
                {"text": f"{'✅' if ch['is_active'] else '❌'} {ch['name']}", "callback_data": f"toggle_{ch['channel_id']}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "channel_menu"}])
        self.send_message(chat_id, "Toggle channel status:", keyboard)
    
    def remove_channel_handler(self, chat_id: int):
        channels = [ch for ch in self.channel_manager.get_all_channels(only_active=False) if not ch['is_default']]
        if not channels:
            self.send_message(chat_id, "No removable channels.")
            return
        keyboard = {"inline_keyboard": []}
        for ch in channels:
            keyboard["inline_keyboard"].append([
                {"text": f"❌ {ch['name']}", "callback_data": f"remove_{ch['channel_id']}"}
            ])
        keyboard["inline_keyboard"].append([{"text": "Back", "callback_data": "channel_menu"}])
        self.send_message(chat_id, "Select channel to remove:", keyboard)
    
    def create_timer_from_forwarded(self, chat_id: int, interval_value: int, interval_unit: str):
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "No message found. Forward a message first.")
            return
        
        pending = self.pending_requests[chat_id]
        channels = self.channel_manager.get_all_channels(only_active=True)
        channel_ids = [ch['channel_id'] for ch in channels]
        channels_json = json.dumps(channel_ids)
        
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO ads (content, ad_type, status, created_at, source_message_id, source_chat_id, media_type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (pending.get('text', ''), 'forward', 'active', datetime.now().isoformat(),
              pending['source_message_id'], pending['source_chat_id'], pending.get('media_type')))
        ad_id = cursor.lastrowid
        
        now = datetime.now()
        if interval_unit == 'minutes':
            first_run = now + timedelta(minutes=interval_value)
        elif interval_unit == 'hours':
            first_run = now + timedelta(hours=interval_value)
        else:
            first_run = now + timedelta(days=interval_value)
        
        cursor.execute('''
            INSERT INTO timers (ad_id, interval_value, interval_unit, next_run, created_at, channel_ids)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (ad_id, interval_value, interval_unit, first_run.isoformat(), now.isoformat(), channels_json))
        timer_id = cursor.lastrowid
        cursor.execute('UPDATE ads SET timer_id = ? WHERE id = ?', (timer_id, ad_id))
        conn.commit()
        conn.close()
        
        self.send_message(chat_id, f"Timer #{timer_id} created!\nEvery {interval_value} {interval_unit}\nFirst run at {first_run.strftime('%H:%M:%S')}")
        self.pending_requests.pop(chat_id, None)
    
    def process_update(self, update: Dict):
        if 'message' in update:
            msg = update['message']
            chat_id = msg['chat']['id']
            user_id = msg.get('from', {}).get('id')
            if 'text' in msg and user_id == self.admin_id:
                self.handle_text_input(chat_id, msg['text'], user_id)
            elif msg.get('forward_date') or msg.get('forward_from') or msg.get('forward_from_chat'):
                self.handle_forwarded_message(msg, chat_id, user_id)
        elif 'callback_query' in update:
            cb = update['callback_query']
            self.handle_callback(cb['data'], cb['message']['chat']['id'], cb['message']['message_id'], cb['from']['id'], cb['id'])
    
    def run_polling(self):
        logging.info("Starting bot in POLLING mode...")
        self.delete_webhook()
        self.timer_manager.start_timer_processor()
        self.schedule_manager.start_scheduler()
        last_update_id = 0
        while self.running:
            try:
                response = requests.get(f"{self.base_url}/getUpdates", params={"offset": last_update_id + 1, "timeout": 30}, timeout=35)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok'):
                        for update in data.get('result', []):
                            last_update_id = update['update_id']
                            self.process_update(update)
                time.sleep(1)
            except Exception as e:
                logging.error(f"Polling error: {e}")
                time.sleep(5)
    
    def run_webhook(self, port: int):
        logging.info(f"Starting bot in WEBHOOK mode on port {port}...")
        self.timer_manager.start_timer_processor()
        self.schedule_manager.start_scheduler()
        render_url = os.environ.get('RENDER_EXTERNAL_URL')
        if render_url:
            self.set_webhook(f"{render_url}/webhook")
        app.run(host='0.0.0.0', port=port)

# ============ FLASK ROUTES ============

@app.route('/webhook', methods=['POST'])
def webhook():
    if bot_instance:
        bot_instance.process_update(request.json)
    return jsonify({"status": "ok"})

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"})

@app.route('/')
def index():
    return jsonify({"message": "Ad Bot is running!"})

# ============ MAIN ============

if __name__ == "__main__":
    BOT_TOKEN = os.environ.get('BOT_TOKEN', "8348531965:AAHA_l_1C_9Hxc2gyakfP29w51If3a8zA28")
    ADMIN_ID = int(os.environ.get('ADMIN_ID', 7049142115))
    PORT = int(os.environ.get('PORT', 8080))
    
    bot_instance = ForwardAdBot(BOT_TOKEN, ADMIN_ID)
    
    if 'RENDER' in os.environ:
        bot_instance.run_webhook(PORT)
    else:
        try:
            bot_instance.run_polling()
        except KeyboardInterrupt:
            bot_instance.running = False
            logging.info("Bot stopped")