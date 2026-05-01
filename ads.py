import requests
import sqlite3
import json
import threading
import time
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from dataclasses import dataclass
from collections import defaultdict
from flask import Flask, request, jsonify
import socket

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
    VOICE = "voice"
    AUDIO = "audio"
    STICKER = "sticker"

class PinDuration(Enum):
    ONE_HOUR = 3600
    THREE_HOURS = 10800
    SIX_HOURS = 21600
    TWELVE_HOURS = 43200
    ONE_DAY = 86400
    THREE_DAYS = 259200
    ONE_WEEK = 604800
    FOREVER = 0

class DeleteSchedule(Enum):
    ONE_HOUR = 3600
    THREE_HOURS = 10800
    SIX_HOURS = 21600
    TWELVE_HOURS = 43200
    ONE_DAY = 86400
    TWO_DAYS = 172800
    THREE_DAYS = 259200
    ONE_WEEK = 604800
    NEVER = 0

# ============ DATABASE SETUP FOR RENDER ============

def get_db_path():
    """Get database path - works on Render and locally"""
    # Render provides /tmp for ephemeral storage, but better to use /data for persistent
    if 'RENDER' in os.environ:
        # Use /data directory for persistent storage on Render
        data_dir = '/data'
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        return os.path.join(data_dir, 'ad_bot.db')
    else:
        return 'ad_bot.db'

# ============ ACCURATE TIMER CLASS ============

class AccurateTimer:
    """Accurate real-time timer system"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.running = True
        self.timer_thread = None
        
    def start_timer_processor(self):
        """Start the timer processing thread"""
        self.timer_thread = threading.Thread(target=self._process_timers, daemon=True)
        self.timer_thread.start()
        logging.info("⏲️ Accurate timer processor started")
    
    def _process_timers(self):
        """Process timers with millisecond accuracy"""
        while self.running:
            try:
                now = datetime.now()
                
                # Use a fresh connection for each query
                conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT t.*, a.content, a.ad_type, a.media_path, a.caption, a.target_channels,
                           a.source_chat_id, a.source_message_id, a.pin, a.pin_duration, a.delete_after
                    FROM timers t
                    JOIN ads a ON t.ad_id = a.id
                    WHERE t.is_active = 1 
                    AND t.next_run <= ?
                    AND (t.max_runs IS NULL OR t.total_runs < t.max_runs)
                    ORDER BY t.next_run ASC
                ''', (now.isoformat(),))
                
                due_timers = cursor.fetchall()
                conn.close()
                
                for timer in due_timers:
                    self._execute_timer(timer)
                
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Timer processor error: {e}")
                time.sleep(5)
    
    def _execute_timer(self, timer):
        """Execute a timer's ad posting"""
        try:
            timer_id = timer[0]
            ad_id = timer[1]
            interval_value = timer[2]
            interval_unit = timer[3]
            next_run = timer[4]
            last_run = timer[5]
            total_runs = timer[6] if len(timer) > 6 else 0
            max_runs = timer[7] if len(timer) > 7 else None
            is_active = timer[8] if len(timer) > 8 else 1
            created_at = timer[9] if len(timer) > 9 else None
            channels_json = timer[10] if len(timer) > 10 else None
            created_from_time = timer[11] if len(timer) > 11 else None
            timer_type = timer[12] if len(timer) > 12 else None
            content = timer[13] if len(timer) > 13 else None
            ad_type = timer[14] if len(timer) > 14 else None
            media_path = timer[15] if len(timer) > 15 else None
            caption = timer[16] if len(timer) > 16 else None
            target_channels_json = timer[17] if len(timer) > 17 else None
            source_chat_id = timer[18] if len(timer) > 18 else None
            source_message_id = timer[19] if len(timer) > 19 else None
            pin = timer[20] if len(timer) > 20 else False
            pin_duration = timer[21] if len(timer) > 21 else 0
            delete_after = timer[22] if len(timer) > 22 else 0
            
            logging.info(f"⏰ Executing timer #{timer_id} (Ad #{ad_id})")
            
            channels = json.loads(channels_json) if channels_json else None
            if not channels:
                channels = [self.bot.channel_manager.get_default_channel()]
            
            for channel in channels:
                if channel:
                    result = self.bot.forward_message_to_channel_with_result(channel, source_chat_id, source_message_id)
                    if result:
                        new_message_id = result
                        self.bot.channel_manager.update_post_count(channel)
                        
                        self.bot.record_posted_message(
                            new_message_id, channel, ad_id, 
                            pin=pin, pin_duration=pin_duration, 
                            delete_after=delete_after
                        )
                        
                        if pin:
                            self.bot.pin_message(channel, new_message_id, pin_duration)
                            if pin_duration > 0:
                                self.bot.schedule_unpin(channel, new_message_id, ad_id, pin_duration)
                        
                        if delete_after > 0:
                            self.bot.schedule_delete(channel, new_message_id, ad_id, delete_after)
                        
                        logging.info(f"✅ Timer #{timer_id} posted to {channel}")
            
            # Update timer stats
            update_conn = sqlite3.connect(get_db_path(), check_same_thread=False)
            update_cursor = update_conn.cursor()
            
            new_total = (total_runs or 0) + 1
            is_active_timer = 0 if max_runs and new_total >= max_runs else 1
            
            update_cursor.execute('''
                UPDATE timers 
                SET last_run = ?, total_runs = ?, is_active = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), new_total, is_active_timer, timer_id))
            update_conn.commit()
            update_conn.close()
            
            if is_active_timer:
                self._update_timer_schedule(timer_id)
                
        except Exception as e:
            logging.error(f"Error executing timer: {e}")
    
    def _update_timer_schedule(self, timer_id):
        """Update the next run time based on interval"""
        try:
            conn = sqlite3.connect(get_db_path(), check_same_thread=False)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT interval_value, interval_unit, next_run, total_runs, max_runs
                FROM timers WHERE id = ?
            ''', (timer_id,))
            
            result = cursor.fetchone()
            if not result:
                conn.close()
                return
            
            interval_value, interval_unit, last_next_run, total_runs, max_runs = result
            
            if isinstance(last_next_run, str):
                last_run_time = datetime.fromisoformat(last_next_run)
            else:
                last_run_time = last_next_run
            
            if interval_unit == 'minutes':
                next_time = last_run_time + timedelta(minutes=interval_value)
            elif interval_unit == 'hours':
                next_time = last_run_time + timedelta(hours=interval_value)
            elif interval_unit == 'days':
                next_time = last_run_time + timedelta(days=interval_value)
            else:
                next_time = last_run_time + timedelta(minutes=interval_value)
            
            is_active = 1
            if max_runs and (total_runs + 1) >= max_runs:
                is_active = 0
            
            cursor.execute('''
                UPDATE timers SET next_run = ?, is_active = ? WHERE id = ?
            ''', (next_time.isoformat(), is_active, timer_id))
            conn.commit()
            conn.close()
            
        except Exception as e:
            logging.error(f"Error updating timer schedule: {e}")

@dataclass
class PostRecord:
    message_id: int
    channel_id: str
    ad_id: int
    posted_at: datetime
    scheduled_delete_at: Optional[datetime]
    scheduled_unpin_at: Optional[datetime]
    is_pinned: bool = False

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
                post_count INTEGER DEFAULT 0,
                auto_pin BOOLEAN DEFAULT 0,
                auto_delete BOOLEAN DEFAULT 0,
                default_pin_duration INTEGER DEFAULT 3600,
                default_delete_after INTEGER DEFAULT 86400,
                max_pinned_posts INTEGER DEFAULT 1,
                settings TEXT
            )
        ')
        
        cursor.execute("PRAGMA table_info(channels)")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        new_columns = {
            'auto_pin': 'BOOLEAN DEFAULT 0',
            'auto_delete': 'BOOLEAN DEFAULT 0',
            'default_pin_duration': 'INTEGER DEFAULT 3600',
            'default_delete_after': 'INTEGER DEFAULT 86400',
            'max_pinned_posts': 'INTEGER DEFAULT 1',
            'settings': 'TEXT'
        }
        
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE channels ADD COLUMN {col_name} {col_type}")
                except Exception as e:
                    logging.warning(f"Could not add {col_name}: {e}")
        
        cursor.execute('SELECT COUNT(*) FROM channels WHERE is_default = 1')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO channels (channel_id, name, is_default, created_at, settings)
                VALUES ('@default', 'Default Channel', 1, ?, '{}')
            ''', (datetime.now().isoformat(),))
        
        conn.commit()
        conn.close()
    
    def add_channel(self, channel_id: str, name: str = None, description: str = None) -> bool:
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO channels (channel_id, name, description, created_at, settings)
                VALUES (?, ?, ?, ?, ?)
            ''', (channel_id, name or channel_id, description, datetime.now().isoformat(), '{}'))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Failed to add channel: {e}")
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
            logging.error(f"Failed to remove channel: {e}")
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
            logging.error(f"Failed to set default channel: {e}")
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
            logging.error(f"Failed to toggle channel: {e}")
            return False
    
    def update_channel_settings(self, channel_id: str, settings: Dict) -> bool:
        try:
            current = self.get_channel_settings(channel_id)
            current.update(settings)
            
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE channels 
                SET auto_pin = ?, auto_delete = ?, default_pin_duration = ?, 
                    default_delete_after = ?, max_pinned_posts = ?, settings = ?
                WHERE channel_id = ?
            ''', (
                1 if current.get('auto_pin', False) else 0,
                1 if current.get('auto_delete', False) else 0,
                current.get('default_pin_duration', 3600),
                current.get('default_delete_after', 86400),
                current.get('max_pinned_posts', 1),
                json.dumps(current),
                channel_id
            ))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logging.error(f"Failed to update channel settings: {e}")
            return False
    
    def get_channel_settings(self, channel_id: str) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT auto_pin, auto_delete, default_pin_duration, default_delete_after, max_pinned_posts, settings, post_count
            FROM channels WHERE channel_id = ?
        ''', (channel_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result and len(result) >= 6:
            return {
                'auto_pin': bool(result[0]),
                'auto_delete': bool(result[1]),
                'default_pin_duration': result[2],
                'default_delete_after': result[3],
                'max_pinned_posts': result[4],
                'settings': json.loads(result[5]) if result[5] else {},
                'post_count': result[6] if len(result) > 6 else 0
            }
        return {
            'auto_pin': False,
            'auto_delete': False,
            'default_pin_duration': 3600,
            'default_delete_after': 86400,
            'max_pinned_posts': 1,
            'settings': {},
            'post_count': 0
        }
    
    def get_all_channels(self, only_active: bool = True) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        query = 'SELECT channel_id, name, is_active, is_default, description, post_count, auto_pin, auto_delete FROM channels'
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
                'description': row[4],
                'post_count': row[5],
                'auto_pin': bool(row[6]) if len(row) > 6 else False,
                'auto_delete': bool(row[7]) if len(row) > 7 else False
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

class ScheduleManager:
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.running = True
        self.scheduler_thread = None
        
    def start_scheduler(self):
        self.scheduler_thread = threading.Thread(target=self._process_scheduled_tasks, daemon=True)
        self.scheduler_thread.start()
        logging.info("📅 Schedule manager started")
    
    def _process_scheduled_tasks(self):
        while self.running:
            try:
                now = datetime.now()
                
                # Process scheduled deletes
                conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, message_id, channel_id, ad_id
                    FROM posted_messages
                    WHERE scheduled_delete_at IS NOT NULL 
                    AND scheduled_delete_at <= ?
                    AND is_deleted = 0
                ''', (now.isoformat(),))
                to_delete = cursor.fetchall()
                conn.close()
                
                for record in to_delete:
                    self._execute_delete(record)
                
                # Process scheduled unpins
                conn2 = sqlite3.connect(get_db_path(), check_same_thread=False)
                cursor2 = conn2.cursor()
                cursor2.execute('''
                    SELECT id, message_id, channel_id, ad_id
                    FROM posted_messages
                    WHERE scheduled_unpin_at IS NOT NULL 
                    AND scheduled_unpin_at <= ?
                    AND is_pinned = 1
                    AND is_deleted = 0
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
            record_id, message_id, channel_id, ad_id = record
            if message_id and channel_id:
                if self.bot.delete_message(channel_id, message_id):
                    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE posted_messages 
                        SET is_deleted = 1, deleted_at = ?
                        WHERE id = ?
                    ''', (datetime.now().isoformat(), record_id))
                    conn.commit()
                    conn.close()
                    logging.info(f"🗑️ Auto-deleted message {message_id} from {channel_id}")
        except Exception as e:
            logging.error(f"Error executing delete: {e}")
    
    def _execute_unpin(self, record):
        try:
            record_id, message_id, channel_id, ad_id = record
            if message_id and channel_id:
                if self.bot.unpin_message(channel_id, message_id):
                    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE posted_messages 
                        SET is_pinned = 0, unpinned_at = ?
                        WHERE id = ?
                    ''', (datetime.now().isoformat(), record_id))
                    conn.commit()
                    conn.close()
                    logging.info(f"📌 Auto-unpinned message {message_id} from {channel_id}")
        except Exception as e:
            logging.error(f"Error executing unpin: {e}")

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
        
        # For webhook mode
        self.webhook_mode = 'RENDER' in os.environ
        
    def init_database(self):
        """Initialize all database tables"""
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        
        # Ads table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT,
                ad_type TEXT,
                status TEXT,
                scheduled_time TEXT,
                created_at TEXT,
                media_path TEXT,
                caption TEXT,
                priority INTEGER DEFAULT 1,
                target_channels TEXT,
                posted_count INTEGER DEFAULT 0,
                source_message_id INTEGER,
                source_chat_id TEXT,
                timer_id INTEGER,
                media_type TEXT,
                media_file_id TEXT,
                pin BOOLEAN DEFAULT 0,
                pin_duration INTEGER DEFAULT 0,
                delete_after INTEGER DEFAULT 0,
                schedule_unpin BOOLEAN DEFAULT 0,
                schedule_delete BOOLEAN DEFAULT 0
            )
        ''')
        
        cursor.execute("PRAGMA table_info(ads)")
        existing_columns = [col[1] for col in cursor.fetchall()]
        
        new_ad_columns = {
            'pin': 'BOOLEAN DEFAULT 0',
            'pin_duration': 'INTEGER DEFAULT 0',
            'delete_after': 'INTEGER DEFAULT 0',
            'schedule_unpin': 'BOOLEAN DEFAULT 0',
            'schedule_delete': 'BOOLEAN DEFAULT 0'
        }
        
        for col_name, col_type in new_ad_columns.items():
            if col_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE ads ADD COLUMN {col_name} {col_type}")
                except Exception as e:
                    logging.warning(f"Could not add {col_name}: {e}")
        
        # Posted messages tracking table
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
                unpinned_at TEXT,
                pin_message_id INTEGER
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_posted_messages_channel ON posted_messages(channel_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_posted_messages_deleted ON posted_messages(is_deleted)')
        
        # Timers table
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
                channel_ids TEXT,
                created_from_time TEXT,
                timer_type TEXT,
                pin BOOLEAN DEFAULT 0,
                pin_duration INTEGER DEFAULT 0,
                delete_after INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute("PRAGMA table_info(timers)")
        existing_timer_columns = [col[1] for col in cursor.fetchall()]
        
        new_timer_columns = {
            'pin': 'BOOLEAN DEFAULT 0',
            'pin_duration': 'INTEGER DEFAULT 0',
            'delete_after': 'INTEGER DEFAULT 0'
        }
        
        for col_name, col_type in new_timer_columns.items():
            if col_name not in existing_timer_columns:
                try:
                    cursor.execute(f"ALTER TABLE timers ADD COLUMN {col_name} {col_type}")
                except Exception as e:
                    logging.warning(f"Could not add {col_name}: {e}")
        
        # Statistics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                date TEXT PRIMARY KEY,
                ads_posted INTEGER DEFAULT 0,
                ads_failed INTEGER DEFAULT 0,
                ads_received INTEGER DEFAULT 0,
                messages_deleted INTEGER DEFAULT 0,
                messages_unpinned INTEGER DEFAULT 0
            )
        ''')
        
        # Analytics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                channel_id TEXT,
                metric_type TEXT,
                value INTEGER,
                metadata TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logging.info(f"Database initialized at {self.db_path}")

    # ============ TELEGRAM API METHODS ============
    
    def api_request(self, method: str, payload: Dict = None, max_retries: int = 3) -> Optional[Dict]:
        """Make API request with retries"""
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
                else:
                    logging.warning(f"API {method} returned {response.status_code}")
                    
            except Exception as e:
                logging.error(f"API request error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
        
        return None
    
    def forward_message_to_channel_with_result(self, channel_id: str, from_chat_id: str, message_id: int) -> Optional[int]:
        if not from_chat_id or not message_id:
            return None
            
        payload = {
            "chat_id": channel_id,
            "from_chat_id": from_chat_id,
            "message_id": message_id
        }
        
        result = self.api_request("forwardMessage", payload)
        if result and result.get('ok'):
            new_message_id = result.get('result', {}).get('message_id')
            return new_message_id
        return None
    
    def pin_message(self, channel_id: str, message_id: int, duration: int = 0, disable_notification: bool = True) -> bool:
        payload = {
            "chat_id": channel_id,
            "message_id": message_id,
            "disable_notification": disable_notification
        }
        result = self.api_request("pinChatMessage", payload)
        return result and result.get('ok')
    
    def unpin_message(self, channel_id: str, message_id: int = None) -> bool:
        payload = {"chat_id": channel_id}
        if message_id:
            payload["message_id"] = message_id
        result = self.api_request("unpinChatMessage", payload)
        return result and result.get('ok')
    
    def delete_message(self, channel_id: str, message_id: int) -> bool:
        payload = {
            "chat_id": channel_id,
            "message_id": message_id
        }
        result = self.api_request("deleteMessage", payload)
        return result and result.get('ok')
    
    def send_message(self, chat_id: int, text: str, keyboard: Dict = None):
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
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
        """Set webhook for the bot"""
        payload = {"url": webhook_url}
        result = self.api_request("setWebhook", payload)
        if result and result.get('ok'):
            logging.info(f"✅ Webhook set to {webhook_url}")
            return True
        else:
            logging.error(f"Failed to set webhook: {result}")
            return False
    
    def delete_webhook(self):
        """Delete webhook (to use polling)"""
        result = self.api_request("deleteWebhook")
        if result and result.get('ok'):
            logging.info("✅ Webhook deleted")
            return True
        return False
    
    # ============ MESSAGE PARSING ============
    
    def extract_message_data(self, message: Dict) -> Dict:
        data = {
            'source_chat_id': message.get('chat', {}).get('id'),
            'source_message_id': message.get('message_id'),
            'text': message.get('text', ''),
            'caption': message.get('caption', ''),
            'media_type': None,
            'media_file_id': None,
            'has_premium_emoji': False
        }
        
        if data['text'] and ('<tg-emoji>' in data['text'] or '⭐' in data['text']):
            data['has_premium_emoji'] = True
        
        if 'photo' in message:
            data['media_type'] = 'photo'
            data['media_file_id'] = message['photo'][-1]['file_id']
            data['text'] = data['caption']
        elif 'video' in message:
            data['media_type'] = 'video'
            data['media_file_id'] = message['video']['file_id']
            data['text'] = data['caption']
        elif 'document' in message:
            data['media_type'] = 'document'
            data['media_file_id'] = message['document']['file_id']
            data['text'] = data['caption']
        
        return data
    
    # ============ HELPER METHODS ============
    
    def _format_duration(self, seconds: int) -> str:
        if seconds <= 0:
            return "Never"
        hours = seconds // 3600
        days = hours // 24
        if days > 0:
            return f"{days} day{'s' if days > 1 else ''}"
        elif hours > 0:
            return f"{hours} hour{'s' if hours > 1 else ''}"
        else:
            return f"{seconds} second{'s' if seconds > 1 else ''}"
    
    # ============ SCHEDULING METHODS ============
    
    def schedule_delete(self, channel_id: str, message_id: int, ad_id: int, delete_after_seconds: int):
        if delete_after_seconds <= 0:
            return
        
        delete_time = datetime.now() + timedelta(seconds=delete_after_seconds)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE posted_messages 
            SET scheduled_delete_at = ?
            WHERE channel_id = ? AND message_id = ? AND ad_id = ?
        ''', (delete_time.isoformat(), channel_id, message_id, ad_id))
        conn.commit()
        conn.close()
    
    def schedule_unpin(self, channel_id: str, message_id: int, ad_id: int, unpin_after_seconds: int):
        if unpin_after_seconds <= 0:
            return
        
        unpin_time = datetime.now() + timedelta(seconds=unpin_after_seconds)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE posted_messages 
            SET scheduled_unpin_at = ?
            WHERE channel_id = ? AND message_id = ? AND ad_id = ?
        ''', (unpin_time.isoformat(), channel_id, message_id, ad_id))
        conn.commit()
        conn.close()
    
    def record_posted_message(self, message_id: int, channel_id: str, ad_id: int, 
                             pin: bool = False, pin_duration: int = 0, 
                             delete_after: int = 0) -> int:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO posted_messages (message_id, channel_id, ad_id, posted_at, is_pinned)
            VALUES (?, ?, ?, ?, ?)
        ''', (message_id, channel_id, ad_id, datetime.now().isoformat(), 1 if pin else 0))
        record_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        if pin and pin_duration > 0:
            self.schedule_unpin(channel_id, message_id, ad_id, pin_duration)
        if delete_after > 0:
            self.schedule_delete(channel_id, message_id, ad_id, delete_after)
        
        return record_id
    
    def record_analytics(self, channel_id: str, metric_type: str, value: int, metadata: Dict = None):
        today = datetime.now().date().isoformat()
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO analytics (date, channel_id, metric_type, value, metadata)
            VALUES (?, ?, ?, ?, ?)
        ''', (today, channel_id, metric_type, value, json.dumps(metadata) if metadata else '{}'))
        conn.commit()
        conn.close()
    
    # ============ SIMPLIFIED HANDLERS FOR DEMO ============
    
    def handle_forwarded_message(self, message: Dict, chat_id: int, user_id: int):
        if user_id != self.admin_id:
            self.send_message(chat_id, "⛔ You're not authorized to use this bot!")
            return
        
        msg_data = self.extract_message_data(message)
        self.pending_requests[chat_id] = msg_data
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "📤 Post Now", "callback_data": "forward_now"}],
                [{"text": "📌 Post & Pin", "callback_data": "forward_pin"}],
                [{"text": "❌ Cancel", "callback_data": "cancel"}]
            ]
        }
        
        preview = msg_data['text'][:200] if msg_data['text'] else f"📷 {msg_data['media_type'] or 'Media'} message"
        text = f"✅ Message Received!\n\nPreview: {preview}\n\nWhat would you like to do?"
        self.send_message(chat_id, text, keyboard)
    
    def execute_forward_enhanced(self, chat_id: int, channel_id: str = None, 
                                 forward_all: bool = False, pin: bool = False, 
                                 pin_duration: int = 0, delete_after: int = 0):
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "❌ No message found.")
            return
        
        pending = self.pending_requests[chat_id]
        
        if forward_all:
            channels = self.channel_manager.get_all_channels(only_active=True)
            channel_ids = [ch['channel_id'] for ch in channels]
        else:
            channel_ids = [channel_id] if channel_id else [self.channel_manager.get_default_channel()]
        
        success_count = 0
        for ch_id in channel_ids:
            new_message_id = self.forward_message_to_channel_with_result(ch_id, pending['source_chat_id'], pending['source_message_id'])
            if new_message_id:
                success_count += 1
                self.channel_manager.update_post_count(ch_id)
                self.record_posted_message(new_message_id, ch_id, 0, pin, pin_duration, delete_after)
                if pin:
                    self.pin_message(ch_id, new_message_id, pin_duration)
        
        result_msg = f"✅ Posted to {success_count}/{len(channel_ids)} channels!"
        self.send_message(chat_id, result_msg)
        self.pending_requests.pop(chat_id, None)
    
    def show_main_menu(self, chat_id: int):
        keyboard = {
            "inline_keyboard": [
                [{"text": "📤 Post Message", "callback_data": "forward_prompt"}],
                [{"text": "🔧 Manage Channels", "callback_data": "channel_menu"}],
                [{"text": "❓ Help", "callback_data": "help_main"}]
            ]
        }
        text = "🤖 Ad Bot - Forward messages to your channels!"
        self.send_message(chat_id, text, keyboard)
    
    def show_channel_menu(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=False)
        keyboard = {
            "inline_keyboard": [
                [{"text": "➕ Add Channel", "callback_data": "ch_add"}],
                [{"text": "📋 List Channels", "callback_data": "ch_list"}],
                [{"text": "◀️ Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        text = f"🔧 Channel Management\n\nTotal Channels: {len(channels)}"
        self.send_message(chat_id, text, keyboard)
    
    def show_help(self, chat_id: int):
        help_text = """
📚 Bot Commands:

/start - Show main menu
/channels - Manage channels
/help - Show this help

How to use:
1. Add your channel using /channels
2. Forward any message to this bot
3. Choose "Post Now" or "Post & Pin"
4. Done!
        """
        self.send_message(chat_id, help_text)
    
    def show_stats(self, chat_id: int):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute('SELECT SUM(ads_posted), SUM(ads_received) FROM stats')
        total_stats = cursor.fetchone()
        conn.close()
        
        text = f"📊 Statistics\n\nTotal Forwarded: {total_stats[0] or 0}\nTotal Received: {total_stats[1] or 0}"
        self.send_message(chat_id, text)
    
    def handle_text_input(self, chat_id: int, text: str, user_id: int):
        if user_id != self.admin_id:
            return
        
        if chat_id in self.pending_requests and self.pending_requests[chat_id].get('action') == 'add_channel':
            parts = text.split(',', 1)
            channel_id = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else channel_id
            
            if self.channel_manager.add_channel(channel_id, name):
                self.send_message(chat_id, f"✅ Channel '{name}' added!")
            else:
                self.send_message(chat_id, f"❌ Failed to add channel")
            
            self.pending_requests.pop(chat_id, None)
            self.show_channel_menu(chat_id)
            return
        
        if text.startswith('/'):
            command = text.split()[0].lower()
            if command == '/start':
                self.show_main_menu(chat_id)
            elif command == '/channels':
                self.show_channel_menu(chat_id)
            elif command == '/stats':
                self.show_stats(chat_id)
            elif command == '/help':
                self.show_help(chat_id)
    
    def handle_callback(self, callback_data: str, chat_id: int, message_id: int, user_id: int, callback_id: str = None):
        if user_id != self.admin_id:
            if callback_id:
                self.answer_callback(callback_id, "Unauthorized!", True)
            return
        
        if callback_data == "forward_prompt":
            self.send_message(chat_id, "📤 Forward any message to me!")
        elif callback_data == "forward_now":
            self.execute_forward_enhanced(chat_id, forward_all=True, pin=False)
        elif callback_data == "forward_pin":
            self.execute_forward_enhanced(chat_id, forward_all=True, pin=True, pin_duration=3600)
        elif callback_data == "channel_menu":
            self.show_channel_menu(chat_id)
        elif callback_data == "ch_add":
            self.send_message(chat_id, "Send channel ID: @username or -1001234567890")
            self.pending_requests[chat_id] = {'action': 'add_channel'}
        elif callback_data == "ch_list":
            channels = self.channel_manager.get_all_channels()
            if channels:
                text = "📋 Your Channels:\n\n" + "\n".join([f"• {ch['name']} - {'✅' if ch['is_active'] else '❌'}" for ch in channels])
            else:
                text = "No channels added yet."
            self.send_message(chat_id, text)
        elif callback_data == "back_to_main":
            self.show_main_menu(chat_id)
        elif callback_data == "help_main":
            self.show_help(chat_id)
        elif callback_data == "cancel":
            self.pending_requests.pop(chat_id, None)
            self.send_message(chat_id, "❌ Cancelled")
        
        if callback_id:
            self.answer_callback(callback_id)
    
    def process_update(self, update: Dict):
        """Process a single update (for webhook mode)"""
        if 'message' in update:
            msg = update['message']
            chat_id = msg['chat']['id']
            user_id = msg.get('from', {}).get('id')
            
            if 'text' in msg and user_id == self.admin_id:
                self.handle_text_input(chat_id, msg['text'], user_id)
            elif 'forward_date' in msg or 'forward_from' in msg or 'forward_from_chat' in msg:
                self.handle_forwarded_message(msg, chat_id, user_id)
        
        elif 'callback_query' in update:
            callback = update['callback_query']
            callback_id = callback['id']
            chat_id = callback['message']['chat']['id']
            message_id = callback['message']['message_id']
            user_id = callback['from']['id']
            data = callback['data']
            self.handle_callback(data, chat_id, message_id, user_id, callback_id)
    
    def run_polling(self):
        """Run bot in polling mode"""
        logging.info("Starting bot in POLLING mode...")
        
        # Delete any existing webhook
        self.delete_webhook()
        
        # Start background services
        self.timer_manager.start_timer_processor()
        self.schedule_manager.start_scheduler()
        
        last_update_id = 0
        
        while self.running:
            try:
                url = f"{self.base_url}/getUpdates"
                params = {"offset": last_update_id + 1, "timeout": 30}
                response = requests.get(url, params=params, timeout=35)
                
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
        """Run bot in webhook mode"""
        logging.info(f"Starting bot in WEBHOOK mode on port {port}...")
        
        # Start background services
        self.timer_manager.start_timer_processor()
        self.schedule_manager.start_scheduler()
        
        # Get Render URL
        render_url = os.environ.get('RENDER_EXTERNAL_URL')
        if render_url:
            webhook_url = f"{render_url}/webhook"
            self.set_webhook(webhook_url)
            logging.info(f"Webhook set to: {webhook_url}")
        
        # Run Flask app
        app.run(host='0.0.0.0', port=port)

# ============ FLASK WEBHOOK ENDPOINT ============

# Global bot instance (will be set after creation)
bot_instance = None

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming webhook updates"""
    if bot_instance:
        update = request.json
        bot_instance.process_update(update)
    return jsonify({"status": "ok"})

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint for Render"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/')
def index():
    """Root endpoint"""
    return jsonify({"message": "Ad Bot is running!", "status": "active"})

# ============ MAIN ENTRY POINT ============

if __name__ == "__main__":
    # Get configuration from environment variables
    BOT_TOKEN = os.environ.get('BOT_TOKEN', "8348531965:AAHA_l_1C_9Hxc2gyakfP29w51If3a8zA28")
    ADMIN_ID = int(os.environ.get('ADMIN_ID', 7049142115))
    PORT = int(os.environ.get('PORT', 8080))
    
    # Create bot instance
    bot_instance = ForwardAdBot(BOT_TOKEN, ADMIN_ID)
    
    # Check if running on Render
    if 'RENDER' in os.environ:
        # Use webhook mode on Render
        bot_instance.run_webhook(PORT)
    else:
        # Use polling mode locally
        try:
            bot_instance.run_polling()
        except KeyboardInterrupt:
            bot_instance.running = False
            logging.info("Bot stopped")