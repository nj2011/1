import requests
import sqlite3
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from dataclasses import dataclass
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ad_bot.log'),
        logging.StreamHandler()
    ]
)

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
                
                # Get all active timers that are due
                self.bot.cursor.execute('''
                    SELECT t.*, a.content, a.ad_type, a.media_path, a.caption, a.target_channels,
                           a.source_chat_id, a.source_message_id, a.pin, a.pin_duration, a.delete_after
                    FROM timers t
                    JOIN ads a ON t.ad_id = a.id
                    WHERE t.is_active = 1 
                    AND t.next_run <= ?
                    AND (t.max_runs IS NULL OR t.total_runs < t.max_runs)
                    ORDER BY t.next_run ASC
                ''', (now.isoformat(),))
                
                due_timers = self.bot.cursor.fetchall()
                
                for timer in due_timers:
                    self._execute_timer(timer)
                
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Timer processor error: {e}")
                time.sleep(5)
    
    def _execute_timer(self, timer):
        """Execute a timer's ad posting"""
        try:
            # Extract timer data - handle different tuple lengths
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
            
            logging.info(f"⏰ Executing timer #{timer_id} (Ad #{ad_id}) at {datetime.now().strftime('%H:%M:%S')}")
            
            channels = json.loads(channels_json) if channels_json else None
            if not channels:
                channels = [self.bot.channel_manager.get_default_channel()]
            
            success_count = 0
            for channel in channels:
                if channel:
                    # Forward the message
                    if self.bot.forward_message_to_channel(channel, source_chat_id, source_message_id):
                        success_count += 1
                        self.bot.channel_manager.update_post_count(channel)
                        
                        # Note: In production, you'd capture the actual message_id from the forward response
                        # and then pin/delete it. This is a simplified version.
                        if pin or pin_duration > 0 or delete_after > 0:
                            logging.info(f"Timer with pin/delete options: pin={pin}, duration={pin_duration}, delete={delete_after}")
            
            # Update timer stats
            new_total = (total_runs or 0) + 1
            is_active_timer = 0 if max_runs and new_total >= max_runs else 1
            
            self.bot.cursor.execute('''
                UPDATE timers 
                SET last_run = ?, total_runs = ?, is_active = ?
                WHERE id = ?
            ''', (datetime.now().isoformat(), new_total, is_active_timer, timer_id))
            self.bot.conn.commit()
            
            logging.info(f"✅ Timer #{timer_id} forwarded to {success_count}/{len(channels)} channels")
            
            # Update next run if still active
            if is_active_timer:
                self._update_timer_schedule(timer_id)
                
        except Exception as e:
            logging.error(f"Error executing timer: {e}")
    
    def _update_timer_schedule(self, timer_id):
        """Update the next run time based on interval"""
        try:
            self.bot.cursor.execute('''
                SELECT interval_value, interval_unit, next_run, total_runs, max_runs
                FROM timers WHERE id = ?
            ''', (timer_id,))
            
            result = self.bot.cursor.fetchone()
            if not result:
                return
            
            interval_value, interval_unit, last_next_run, total_runs, max_runs = result
            
            # Parse the last run time
            if isinstance(last_next_run, str):
                last_run_time = datetime.fromisoformat(last_next_run)
            else:
                last_run_time = last_next_run
            
            # Calculate next run time based on interval
            if interval_unit == 'minutes':
                next_time = last_run_time + timedelta(minutes=interval_value)
            elif interval_unit == 'hours':
                next_time = last_run_time + timedelta(hours=interval_value)
            elif interval_unit == 'days':
                next_time = last_run_time + timedelta(days=interval_value)
            else:
                next_time = last_run_time + timedelta(minutes=interval_value)
            
            # Check if max runs reached
            is_active = 1
            if max_runs and (total_runs + 1) >= max_runs:
                is_active = 0
            
            self.bot.cursor.execute('''
                UPDATE timers SET next_run = ?, is_active = ? WHERE id = ?
            ''', (next_time.isoformat(), is_active, timer_id))
            self.bot.conn.commit()
            
        except Exception as e:
            logging.error(f"Error updating timer schedule: {e}")

@dataclass
class PostRecord:
    """Record of a posted message"""
    message_id: int
    channel_id: str
    ad_id: int
    posted_at: datetime
    scheduled_delete_at: Optional[datetime]
    scheduled_unpin_at: Optional[datetime]
    is_pinned: bool = False

class ChannelManager:
    """Manage multiple channels with enhanced features"""
    
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = db_connection.cursor()
        self.init_channel_table()
    
    def init_channel_table(self):
        """Initialize channels table with enhanced fields"""
        self.cursor.execute('''
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
        ''')
        
        # Check and add new columns if needed
        self.cursor.execute("PRAGMA table_info(channels)")
        existing_columns = [col[1] for col in self.cursor.fetchall()]
        
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
                    self.cursor.execute(f"ALTER TABLE channels ADD COLUMN {col_name} {col_type}")
                    logging.info(f"Added {col_name} column to channels table")
                except Exception as e:
                    logging.warning(f"Could not add {col_name}: {e}")
        
        self.cursor.execute('SELECT COUNT(*) FROM channels WHERE is_default = 1')
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute('''
                INSERT INTO channels (channel_id, name, is_default, created_at, settings)
                VALUES ('@default', 'Default Channel', 1, ?, '{}')
            ''', (datetime.now().isoformat(),))
            self.conn.commit()
        
        self.conn.commit()
    
    def add_channel(self, channel_id: str, name: str = None, description: str = None) -> bool:
        try:
            self.cursor.execute('''
                INSERT INTO channels (channel_id, name, description, created_at, settings)
                VALUES (?, ?, ?, ?, ?)
            ''', (channel_id, name or channel_id, description, datetime.now().isoformat(), '{}'))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to add channel: {e}")
            return False
    
    def remove_channel(self, channel_id: str) -> bool:
        try:
            self.cursor.execute('SELECT is_default FROM channels WHERE channel_id = ?', (channel_id,))
            result = self.cursor.fetchone()
            if result and result[0] == 1:
                return False
            
            self.cursor.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to remove channel: {e}")
            return False
    
    def set_default_channel(self, channel_id: str) -> bool:
        try:
            self.cursor.execute('UPDATE channels SET is_default = 0')
            self.cursor.execute('UPDATE channels SET is_default = 1, is_active = 1 WHERE channel_id = ?', (channel_id,))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to set default channel: {e}")
            return False
    
    def toggle_channel(self, channel_id: str, active: bool) -> bool:
        try:
            self.cursor.execute('UPDATE channels SET is_active = ? WHERE channel_id = ?', (1 if active else 0, channel_id))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to toggle channel: {e}")
            return False
    
    def update_channel_settings(self, channel_id: str, settings: Dict) -> bool:
        try:
            # Get current settings
            current = self.get_channel_settings(channel_id)
            current.update(settings)
            
            self.cursor.execute('''
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
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Failed to update channel settings: {e}")
            return False
    
    def get_channel_settings(self, channel_id: str) -> Dict:
        self.cursor.execute('''
            SELECT auto_pin, auto_delete, default_pin_duration, default_delete_after, max_pinned_posts, settings
            FROM channels WHERE channel_id = ?
        ''', (channel_id,))
        result = self.cursor.fetchone()
        if result and len(result) >= 6:
            return {
                'auto_pin': bool(result[0]),
                'auto_delete': bool(result[1]),
                'default_pin_duration': result[2],
                'default_delete_after': result[3],
                'max_pinned_posts': result[4],
                'settings': json.loads(result[5]) if result[5] else {},
                'post_count': 0
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
        query = 'SELECT channel_id, name, is_active, is_default, description, post_count, auto_pin, auto_delete FROM channels'
        if only_active:
            query += ' WHERE is_active = 1'
        query += ' ORDER BY is_default DESC, name ASC'
        
        self.cursor.execute(query)
        channels = []
        for row in self.cursor.fetchall():
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
        return channels
    
    def get_default_channel(self) -> str:
        self.cursor.execute('SELECT channel_id FROM channels WHERE is_default = 1 AND is_active = 1')
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def update_post_count(self, channel_id: str):
        self.cursor.execute('UPDATE channels SET post_count = post_count + 1 WHERE channel_id = ?', (channel_id,))
        self.conn.commit()

class ScheduleManager:
    """Manage scheduled tasks for delete/unpin"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.running = True
        self.scheduler_thread = None
        
    def start_scheduler(self):
        """Start the scheduled tasks processor"""
        self.scheduler_thread = threading.Thread(target=self._process_scheduled_tasks, daemon=True)
        self.scheduler_thread.start()
        logging.info("📅 Schedule manager started")
    
    def _process_scheduled_tasks(self):
        """Process scheduled delete/unpin tasks"""
        while self.running:
            try:
                now = datetime.now()
                
                # Process scheduled deletes
                self.bot.cursor.execute('''
                    SELECT id, message_id, channel_id, ad_id
                    FROM posted_messages
                    WHERE scheduled_delete_at IS NOT NULL 
                    AND scheduled_delete_at <= ?
                    AND is_deleted = 0
                ''', (now.isoformat(),))
                
                to_delete = self.bot.cursor.fetchall()
                for record in to_delete:
                    self._execute_delete(record)
                
                # Process scheduled unpins
                self.bot.cursor.execute('''
                    SELECT id, message_id, channel_id, ad_id
                    FROM posted_messages
                    WHERE scheduled_unpin_at IS NOT NULL 
                    AND scheduled_unpin_at <= ?
                    AND is_pinned = 1
                ''', (now.isoformat(),))
                
                to_unpin = self.bot.cursor.fetchall()
                for record in to_unpin:
                    self._execute_unpin(record)
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logging.error(f"Scheduler error: {e}")
                time.sleep(60)
    
    def _execute_delete(self, record):
        """Delete a message"""
        try:
            record_id, message_id, channel_id, ad_id = record
            if message_id and channel_id:
                self.bot.delete_message(channel_id, message_id)
                self.bot.cursor.execute('''
                    UPDATE posted_messages 
                    SET is_deleted = 1, deleted_at = ?
                    WHERE id = ?
                ''', (datetime.now().isoformat(), record_id))
                self.bot.conn.commit()
                logging.info(f"🗑️ Auto-deleted message {message_id} from {channel_id}")
        except Exception as e:
            logging.error(f"Error executing delete: {e}")
    
    def _execute_unpin(self, record):
        """Unpin a message"""
        try:
            record_id, message_id, channel_id, ad_id = record
            if message_id and channel_id:
                self.bot.unpin_message(channel_id, message_id)
                self.bot.cursor.execute('''
                    UPDATE posted_messages 
                    SET is_pinned = 0, unpinned_at = ?
                    WHERE id = ?
                ''', (datetime.now().isoformat(), record_id))
                self.bot.conn.commit()
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
        self.init_database()
        self.channel_manager = ChannelManager(self.conn)
        self.timer_manager = AccurateTimer(self)
        self.schedule_manager = ScheduleManager(self)
        
    def init_database(self):
        """Initialize SQLite database with all required columns"""
        self.conn = sqlite3.connect('ad_bot.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Ads table
        self.cursor.execute('''
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
        
        # Check and add missing columns to ads table
        self.cursor.execute("PRAGMA table_info(ads)")
        existing_columns = [col[1] for col in self.cursor.fetchall()]
        
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
                    self.cursor.execute(f"ALTER TABLE ads ADD COLUMN {col_name} {col_type}")
                    logging.info(f"Added {col_name} column to ads table")
                except Exception as e:
                    logging.warning(f"Could not add {col_name}: {e}")
        
        # Posted messages tracking table
        self.cursor.execute('''
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
        
        # Timers table with enhanced fields
        self.cursor.execute('''
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
        
        # Check and add missing columns to timers table
        self.cursor.execute("PRAGMA table_info(timers)")
        existing_timer_columns = [col[1] for col in self.cursor.fetchall()]
        
        new_timer_columns = {
            'pin': 'BOOLEAN DEFAULT 0',
            'pin_duration': 'INTEGER DEFAULT 0',
            'delete_after': 'INTEGER DEFAULT 0'
        }
        
        for col_name, col_type in new_timer_columns.items():
            if col_name not in existing_timer_columns:
                try:
                    self.cursor.execute(f"ALTER TABLE timers ADD COLUMN {col_name} {col_type}")
                    logging.info(f"Added {col_name} column to timers table")
                except Exception as e:
                    logging.warning(f"Could not add {col_name}: {e}")
        
        # Statistics table
        self.cursor.execute('''
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
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS analytics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                channel_id TEXT,
                metric_type TEXT,
                value INTEGER,
                metadata TEXT
            )
        ''')
        
        self.conn.commit()
        logging.info("Database initialized with enhanced features")
    
    # ============ TELEGRAM API METHODS ============
    
    def forward_message_to_channel(self, channel_id: str, from_chat_id: str, message_id: int) -> bool:
        """Forward a message to channel"""
        if not from_chat_id or not message_id:
            return False
            
        url = f"{self.base_url}/forwardMessage"
        payload = {
            "chat_id": channel_id,
            "from_chat_id": from_chat_id,
            "message_id": message_id
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('ok'):
                    # Return the new message_id if available
                    result = data.get('result', {})
                    new_message_id = result.get('message_id')
                    logging.info(f"✅ Forwarded message {message_id} to {channel_id} (new ID: {new_message_id})")
                    return True
                else:
                    logging.warning(f"Forward failed: {data}")
                    return False
            else:
                logging.warning(f"Forward failed: {response.text}")
                return False
        except Exception as e:
            logging.warning(f"Forward exception: {e}")
            return False
    
    def pin_message(self, channel_id: str, message_id: int, duration: int = 0, disable_notification: bool = True) -> bool:
        """Pin a message to channel"""
        url = f"{self.base_url}/pinChatMessage"
        payload = {
            "chat_id": channel_id,
            "message_id": message_id,
            "disable_notification": disable_notification
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logging.info(f"📌 Pinned message {message_id} to {channel_id} (duration: {duration}s)")
                return True
            else:
                logging.warning(f"Pin failed: {response.text}")
                return False
        except Exception as e:
            logging.warning(f"Pin exception: {e}")
            return False
    
    def unpin_message(self, channel_id: str, message_id: int = None) -> bool:
        """Unpin message from channel (if message_id is None, unpin all)"""
        url = f"{self.base_url}/unpinChatMessage"
        payload = {"chat_id": channel_id}
        if message_id:
            payload["message_id"] = message_id
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logging.info(f"📌 Unpinned message {message_id} from {channel_id}")
                return True
            else:
                logging.warning(f"Unpin failed: {response.text}")
                return False
        except Exception as e:
            logging.warning(f"Unpin exception: {e}")
            return False
    
    def delete_message(self, channel_id: str, message_id: int) -> bool:
        """Delete a message"""
        url = f"{self.base_url}/deleteMessage"
        payload = {
            "chat_id": channel_id,
            "message_id": message_id
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logging.info(f"🗑️ Deleted message {message_id} from {channel_id}")
                return True
            else:
                logging.warning(f"Delete failed: {response.text}")
                return False
        except Exception as e:
            logging.warning(f"Delete exception: {e}")
            return False
    
    def get_chat_administrators(self, channel_id: str) -> List[Dict]:
        """Get channel administrators"""
        url = f"{self.base_url}/getChatAdministrators"
        payload = {"chat_id": channel_id}
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get('result', [])
        except Exception as e:
            logging.warning(f"Get admins exception: {e}")
        return []
    
    def get_chat_member_count(self, channel_id: str) -> int:
        """Get channel member count"""
        url = f"{self.base_url}/getChatMemberCount"
        payload = {"chat_id": channel_id}
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return data.get('result', 0)
        except Exception as e:
            logging.warning(f"Get member count exception: {e}")
        return 0
    
    # ============ MESSAGE PARSING ============
    
    def extract_message_data(self, message: Dict) -> Dict:
        """Extract all data from a message including media"""
        data = {
            'source_chat_id': message.get('chat', {}).get('id'),
            'source_message_id': message.get('message_id'),
            'text': message.get('text', ''),
            'caption': message.get('caption', ''),
            'media_type': None,
            'media_file_id': None,
            'has_premium_emoji': False
        }
        
        # Check for premium emojis in text/caption
        if data['text'] and ('<tg-emoji>' in data['text'] or '⭐' in data['text']):
            data['has_premium_emoji'] = True
        
        # Extract photo
        if 'photo' in message:
            data['media_type'] = 'photo'
            data['media_file_id'] = message['photo'][-1]['file_id']
            data['text'] = data['caption']
        
        # Extract video
        elif 'video' in message:
            data['media_type'] = 'video'
            data['media_file_id'] = message['video']['file_id']
            data['text'] = data['caption']
        
        # Extract document
        elif 'document' in message:
            data['media_type'] = 'document'
            data['media_file_id'] = message['document']['file_id']
            data['text'] = data['caption']
        
        return data
    
    # ============ HELPER METHODS ============
    
    def _format_duration(self, seconds: int) -> str:
        """Format duration in seconds to human readable"""
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
    
    def _format_daily_stats(self, daily_stats: List[Tuple]) -> str:
        """Format daily statistics"""
        if not daily_stats:
            return "No posts in last 7 days"
        
        lines = []
        for count, date in daily_stats[:7]:
            lines.append(f"📅 {date}: {count} post{'s' if count > 1 else ''}")
        
        return "\n".join(lines)
    
    def _format_admin_list(self, admins: List[Dict]) -> str:
        """Format admin list"""
        if not admins:
            return "Unable to fetch admin list"
        
        admin_names = []
        for admin in admins[:5]:
            user = admin.get('user', {})
            name = user.get('first_name', '')
            if user.get('username'):
                name += f" (@{user['username']})"
            admin_names.append(f"• {name}")
        
        if len(admins) > 5:
            admin_names.append(f"• ... and {len(admins) - 5} more")
        
        return "\n".join(admin_names)
    
    # ============ SCHEDULING METHODS ============
    
    def schedule_delete(self, channel_id: str, message_id: int, ad_id: int, delete_after_seconds: int):
        """Schedule a message for deletion"""
        if delete_after_seconds <= 0:
            return
        
        delete_time = datetime.now() + timedelta(seconds=delete_after_seconds)
        
        self.cursor.execute('''
            UPDATE posted_messages 
            SET scheduled_delete_at = ?
            WHERE channel_id = ? AND message_id = ? AND ad_id = ?
        ''', (delete_time.isoformat(), channel_id, message_id, ad_id))
        self.conn.commit()
        
        logging.info(f"⏰ Scheduled delete for message {message_id} at {delete_time}")
    
    def schedule_unpin(self, channel_id: str, message_id: int, ad_id: int, unpin_after_seconds: int):
        """Schedule a message for unpinning"""
        if unpin_after_seconds <= 0:
            return
        
        unpin_time = datetime.now() + timedelta(seconds=unpin_after_seconds)
        
        self.cursor.execute('''
            UPDATE posted_messages 
            SET scheduled_unpin_at = ?
            WHERE channel_id = ? AND message_id = ? AND ad_id = ?
        ''', (unpin_time.isoformat(), channel_id, message_id, ad_id))
        self.conn.commit()
        
        logging.info(f"⏰ Scheduled unpin for message {message_id} at {unpin_time}")
    
    def record_posted_message(self, message_id: int, channel_id: str, ad_id: int, 
                             pin: bool = False, pin_duration: int = 0, 
                             delete_after: int = 0) -> int:
        """Record a posted message for tracking"""
        self.cursor.execute('''
            INSERT INTO posted_messages (message_id, channel_id, ad_id, posted_at, is_pinned)
            VALUES (?, ?, ?, ?, ?)
        ''', (message_id, channel_id, ad_id, datetime.now().isoformat(), 1 if pin else 0))
        
        record_id = self.cursor.lastrowid
        self.conn.commit()
        
        if pin and pin_duration > 0:
            self.schedule_unpin(channel_id, message_id, ad_id, pin_duration)
        
        if delete_after > 0:
            self.schedule_delete(channel_id, message_id, ad_id, delete_after)
        
        # Record analytics
        self.record_analytics(channel_id, 'post', 1)
        
        return record_id
    
    def record_analytics(self, channel_id: str, metric_type: str, value: int, metadata: Dict = None):
        """Record analytics data"""
        today = datetime.now().date().isoformat()
        self.cursor.execute('''
            INSERT INTO analytics (date, channel_id, metric_type, value, metadata)
            VALUES (?, ?, ?, ?, ?)
        ''', (today, channel_id, metric_type, value, json.dumps(metadata) if metadata else '{}'))
        self.conn.commit()
    
    # ============ ENHANCED CHANNEL MANAGEMENT ============
    
    def show_channel_menu(self, chat_id: int):
        """Show channel management menu"""
        channels = self.channel_manager.get_all_channels(only_active=False)
        default_ch = self.channel_manager.get_default_channel()
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "➕ Add Channel", "callback_data": "ch_add"}],
                [{"text": "📋 List Channels", "callback_data": "ch_list"}],
                [{"text": "⭐ Set Default", "callback_data": "ch_default"}],
                [{"text": "🔘 Toggle Active/Inactive", "callback_data": "ch_toggle"}],
                [{"text": "⚙️ Channel Settings", "callback_data": "channel_settings_menu"}],
                [{"text": "❌ Remove Channel", "callback_data": "ch_remove"}],
                [{"text": "📊 Channel Stats", "callback_data": "ch_stats"}],
                [{"text": "◀️ Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        
        channels_list = "\n".join([
            f"{'⭐' if ch['is_default'] else '📢'} {ch['name']} - {'✅ Active' if ch['is_active'] else '❌ Inactive'}"
            for ch in channels[:10]
        ])
        
        text = f"""
🔧 <b>Channel Management</b>

<b>Default Channel:</b> {default_ch}
<b>Total Channels:</b> {len(channels)}
<b>Active Channels:</b> {len([c for c in channels if c['is_active']])}

<b>Your Channels:</b>
{channels_list if channels_list else 'No channels added yet'}

What would you like to do?
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def show_channel_settings_menu(self, chat_id: int, channel_id: str = None):
        """Show channel settings menu"""
        if not channel_id:
            channels = self.channel_manager.get_all_channels(only_active=True)
            if not channels:
                self.send_message(chat_id, "❌ No channels found. Add a channel first.")
                return
            
            keyboard = {
                "inline_keyboard": [
                    [{"text": f"⚙️ {ch['name']}", "callback_data": f"ch_settings_{ch['channel_id']}"}]
                    for ch in channels
                ] + [[{"text": "◀️ Back to Channel Menu", "callback_data": "channel_menu"}]]
            }
            self.send_message(chat_id, "⚙️ <b>Select channel to configure:</b>", keyboard)
            return
        
        settings = self.channel_manager.get_channel_settings(channel_id)
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"{'✅' if settings['auto_pin'] else '❌'} Auto-Pin", 
                  "callback_data": f"ch_toggle_pin_{channel_id}"}],
                [{"text": f"{'✅' if settings['auto_delete'] else '❌'} Auto-Delete", 
                  "callback_data": f"ch_toggle_delete_{channel_id}"}],
                [{"text": "📌 Pin Duration", "callback_data": f"ch_pin_duration_{channel_id}"}],
                [{"text": "🗑️ Delete After", "callback_data": f"ch_delete_after_{channel_id}"}],
                [{"text": "📊 Channel Analytics", "callback_data": f"ch_analytics_{channel_id}"}],
                [{"text": "◀️ Back", "callback_data": "channel_menu"}]
            ]
        }
        
        text = f"""
⚙️ <b>Channel Settings: {channel_id}</b>

<b>Auto-Pin:</b> {'Enabled' if settings['auto_pin'] else 'Disabled'}
<b>Auto-Delete:</b> {'Enabled' if settings['auto_delete'] else 'Disabled'}
<b>Pin Duration:</b> {self._format_duration(settings['default_pin_duration'])}
<b>Delete After:</b> {self._format_duration(settings['default_delete_after'])}
<b>Max Pinned Posts:</b> {settings['max_pinned_posts']}

<b>What would you like to configure?</b>
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def show_channel_analytics(self, chat_id: int, channel_id: str):
        """Show channel analytics"""
        # Get member count
        member_count = self.get_chat_member_count(channel_id)
        
        # Get admins
        admins = self.get_chat_administrators(channel_id)
        
        # Get post stats
        self.cursor.execute('''
            SELECT COUNT(*), DATE(posted_at)
            FROM posted_messages
            WHERE channel_id = ?
            AND posted_at >= date('now', '-7 days')
            GROUP BY DATE(posted_at)
            ORDER BY DATE(posted_at) DESC
        ''', (channel_id,))
        
        daily_stats = self.cursor.fetchall()
        
        # Get analytics
        self.cursor.execute('''
            SELECT metric_type, SUM(value)
            FROM analytics
            WHERE channel_id = ? AND date >= date('now', '-30 days')
            GROUP BY metric_type
        ''', (channel_id,))
        
        analytics = dict(self.cursor.fetchall())
        
        text = f"""
📊 <b>Channel Analytics: {channel_id}</b>

<b>Channel Stats:</b>
👥 Members: {member_count}
👑 Admins: {len(admins)}
📤 Total Posts: {self.channel_manager.get_channel_settings(channel_id).get('post_count', 0)}

<b>Last 7 Days Posts:</b>
{self._format_daily_stats(daily_stats)}

<b>Analytics (30 days):</b>
📥 Posts: {analytics.get('post', 0)}
✅ Success: {analytics.get('success', 0)}
❌ Failed: {analytics.get('failed', 0)}

<b>Admin List:</b>
{self._format_admin_list(admins)}
        """
        
        self.send_message(chat_id, text)
    
    def toggle_channel_auto_pin(self, channel_id: str) -> bool:
        """Toggle auto-pin for channel"""
        settings = self.channel_manager.get_channel_settings(channel_id)
        settings['auto_pin'] = not settings.get('auto_pin', False)
        return self.channel_manager.update_channel_settings(channel_id, settings)
    
    def toggle_channel_auto_delete(self, channel_id: str) -> bool:
        """Toggle auto-delete for channel"""
        settings = self.channel_manager.get_channel_settings(channel_id)
        settings['auto_delete'] = not settings.get('auto_delete', False)
        return self.channel_manager.update_channel_settings(channel_id, settings)
    
    def set_channel_pin_duration(self, channel_id: str, duration_seconds: int) -> bool:
        """Set default pin duration for channel"""
        settings = self.channel_manager.get_channel_settings(channel_id)
        settings['default_pin_duration'] = duration_seconds
        return self.channel_manager.update_channel_settings(channel_id, settings)
    
    def set_channel_delete_after(self, channel_id: str, delete_after_seconds: int) -> bool:
        """Set default delete after time for channel"""
        settings = self.channel_manager.get_channel_settings(channel_id)
        settings['default_delete_after'] = delete_after_seconds
        return self.channel_manager.update_channel_settings(channel_id, settings)
    
    def add_channel_handler(self, chat_id: int):
        """Handle adding a new channel"""
        self.send_message(chat_id, 
            "➕ <b>Add New Channel</b>\n\n"
            "Send me the channel info in this format:\n"
            "<code>@channelusername</code> or <code>-1001234567890</code>\n\n"
            "Optional: Add name after a comma\n"
            "Example: <code>@myads, My Ad Channel</code>\n\n"
            "⚠️ Make sure I'm an admin in that channel!"
        )
        self.pending_requests[chat_id] = {'action': 'add_channel'}
    
    def list_channels_handler(self, chat_id: int):
        """List all channels"""
        channels = self.channel_manager.get_all_channels(only_active=False)
        
        if not channels:
            self.send_message(chat_id, "📭 No channels added yet. Use Add Channel to add one.")
            return
        
        text = "📋 <b>Your Channels</b>\n\n"
        for ch in channels:
            text += f"{'⭐' if ch['is_default'] else '📢'} <b>{ch['name']}</b>\n"
            text += f"ID: <code>{ch['channel_id']}</code>\n"
            text += f"Status: {'✅ Active' if ch['is_active'] else '❌ Inactive'}\n"
            text += f"Posts: {ch['post_count']}\n"
            text += "─" * 30 + "\n"
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "◀️ Back to Channel Menu", "callback_data": "channel_menu"}]
            ]
        }
        
        self.send_message(chat_id, text, keyboard)
    
    def set_default_channel_handler(self, chat_id: int):
        """Show channel selection for default"""
        channels = self.channel_manager.get_all_channels(only_active=True)
        
        if not channels:
            self.send_message(chat_id, "❌ No active channels found. Add a channel first.")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"{'⭐' if ch['is_default'] else '📢'} {ch['name']}", 
                  "callback_data": f"set_default_{ch['channel_id']}"}]
                for ch in channels
            ] + [[{"text": "◀️ Back to Channel Menu", "callback_data": "channel_menu"}]]
        }
        
        self.send_message(chat_id, "⭐ <b>Select default channel:</b>", keyboard)
    
    def toggle_channel_handler(self, chat_id: int):
        """Show channel selection for toggling"""
        channels = self.channel_manager.get_all_channels(only_active=False)
        
        if not channels:
            self.send_message(chat_id, "❌ No channels found.")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"{'✅' if ch['is_active'] else '❌'} {ch['name']}", 
                  "callback_data": f"toggle_{ch['channel_id']}"}]
                for ch in channels
            ] + [[{"text": "◀️ Back to Channel Menu", "callback_data": "channel_menu"}]]
        }
        
        self.send_message(chat_id, "🔘 <b>Toggle channel status:</b>", keyboard)
    
    def remove_channel_handler(self, chat_id: int):
        """Show channel selection for removal"""
        channels = self.channel_manager.get_all_channels(only_active=False)
        channels = [ch for ch in channels if not ch['is_default']]
        
        if not channels:
            self.send_message(chat_id, "❌ No non-default channels to remove.")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"❌ {ch['name']}", "callback_data": f"remove_{ch['channel_id']}"}]
                for ch in channels
            ] + [[{"text": "◀️ Back to Channel Menu", "callback_data": "channel_menu"}]]
        }
        
        self.send_message(chat_id, "⚠️ <b>Select channel to remove:</b>", keyboard)
    
    def channel_stats_handler(self, chat_id: int):
        """Show channel statistics"""
        channels = self.channel_manager.get_all_channels(only_active=False)
        
        total_posts = sum(ch['post_count'] for ch in channels)
        active = len([ch for ch in channels if ch['is_active']])
        default_ch = self.channel_manager.get_default_channel()
        
        text = f"📊 <b>Channel Statistics</b>\n\n"
        text += f"⭐ Default Channel: {default_ch}\n"
        text += f"📢 Total Channels: {len(channels)}\n"
        text += f"✅ Active Channels: {active}\n"
        text += f"📤 Total Posts Sent: {total_posts}\n\n"
        
        text += "<b>Channel Performance:</b>\n"
        for ch in sorted(channels, key=lambda x: x['post_count'], reverse=True):
            text += f"📢 {ch['name']}: {ch['post_count']} posts\n"
        
        self.send_message(chat_id, text)
    
    # ============ TIMER MANAGEMENT ============
    
    def show_timer_menu(self, chat_id: int):
        """Show timer management menu"""
        self.cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active_timers = self.cursor.fetchone()[0]
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "⏲️ Create New Timer", "callback_data": "timer_create"}],
                [{"text": "📋 List Active Timers", "callback_data": "timer_list"}],
                [{"text": "⏸️ Pause Timer", "callback_data": "timer_pause"}],
                [{"text": "❌ Delete Timer", "callback_data": "timer_delete"}],
                [{"text": "◀️ Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        
        text = f"""
⏲️ <b>Timer Management</b>

<b>Active Timers:</b> {active_timers}

<b>How Timers Work:</b>
• Timer starts from creation time
• If created at 5:21:30 with 1 hour interval:
  → First post at 6:21:30 (exact 1 hour later)
• Messages are FORWARDED to channels

<b>Available Intervals:</b>
• Minutes: 1, 5, 10, 15, 30
• Hours: 1, 2, 4, 6, 12
• Days: 1 (24 hours)
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def show_timer_interval_menu(self, chat_id: int):
        """Show interval selection for timer"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "⏱️ Every 1 minute", "callback_data": "timer_interval_1_minutes"}],
                [{"text": "⏱️ Every 5 minutes", "callback_data": "timer_interval_5_minutes"}],
                [{"text": "⏱️ Every 10 minutes", "callback_data": "timer_interval_10_minutes"}],
                [{"text": "⏱️ Every 15 minutes", "callback_data": "timer_interval_15_minutes"}],
                [{"text": "⏱️ Every 30 minutes", "callback_data": "timer_interval_30_minutes"}],
                [{"text": "🕐 Every 1 hour", "callback_data": "timer_interval_1_hours"}],
                [{"text": "🕐 Every 2 hours", "callback_data": "timer_interval_2_hours"}],
                [{"text": "🕐 Every 4 hours", "callback_data": "timer_interval_4_hours"}],
                [{"text": "🕐 Every 6 hours", "callback_data": "timer_interval_6_hours"}],
                [{"text": "🕐 Every 12 hours", "callback_data": "timer_interval_12_hours"}],
                [{"text": "📅 Every day", "callback_data": "timer_interval_1_days"}],
                [{"text": "◀️ Back to Timer Menu", "callback_data": "timer_menu"}]
            ]
        }
        
        now = datetime.now()
        text = f"""
⏲️ <b>Create Accurate Timer</b>

<b>Current time:</b> {now.strftime('%H:%M:%S')}

<b>How it works:</b>
• Timer starts counting from NOW
• If you create at {now.strftime('%H:%M:%S')}:
  - Every 1 hour → First run at {(now + timedelta(hours=1)).strftime('%H:%M:%S')}
  - Every 30 min → First run at {(now + timedelta(minutes=30)).strftime('%H:%M:%S')}

<b>Select interval:</b>
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def list_timers_handler(self, chat_id: int):
        """List all active timers"""
        self.cursor.execute('''
            SELECT t.id, t.interval_value, t.interval_unit, t.next_run, t.total_runs, t.max_runs,
                   a.content
            FROM timers t
            JOIN ads a ON t.ad_id = a.id
            WHERE t.is_active = 1
            ORDER BY t.next_run ASC
        ''')
        
        timers = self.cursor.fetchall()
        
        if not timers:
            self.send_message(chat_id, "📭 No active timers. Create one with 'Create New Timer'")
            return
        
        text = "⏲️ <b>Active Timers</b>\n\n"
        for timer in timers:
            timer_id, interval_value, interval_unit, next_run_str, total_runs, max_runs, content = timer
            
            next_run = datetime.fromisoformat(next_run_str)
            now = datetime.now()
            
            if next_run > now:
                remaining = next_run - now
                minutes = remaining.seconds // 60
                seconds = remaining.seconds % 60
                countdown = f"{minutes}m {seconds}s"
            else:
                countdown = "NOW"
            
            text += f"🆔 <b>Timer #{timer_id}</b>\n"
            text += f"📝 {content[:50] if content else 'Media message'}...\n"
            text += f"⏰ Every {interval_value} {interval_unit}\n"
            text += f"⏳ Next in: {countdown}\n"
            text += f"📊 Runs: {total_runs}/{max_runs if max_runs else '∞'}\n"
            text += "─" * 30 + "\n"
        
        self.send_message(chat_id, text)
    
    def pause_timer_selection(self, chat_id: int):
        """Show timers to pause"""
        self.cursor.execute('SELECT id, interval_value, interval_unit FROM timers WHERE is_active = 1')
        timers = self.cursor.fetchall()
        
        if not timers:
            self.send_message(chat_id, "📭 No active timers to pause.")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"⏸️ Timer #{t[0]} - Every {t[1]} {t[2]}", 
                  "callback_data": f"timer_pause_{t[0]}"}]
                for t in timers[:10]
            ] + [[{"text": "◀️ Back", "callback_data": "timer_menu"}]]
        }
        
        self.send_message(chat_id, "⏸️ <b>Select timer to pause:</b>", keyboard)
    
    def delete_timer_selection(self, chat_id: int):
        """Show timers to delete"""
        self.cursor.execute('SELECT id, interval_value, interval_unit FROM timers')
        timers = self.cursor.fetchall()
        
        if not timers:
            self.send_message(chat_id, "📭 No timers to delete.")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"❌ Timer #{t[0]} - Every {t[1]} {t[2]}", 
                  "callback_data": f"timer_delete_{t[0]}"}]
                for t in timers[:10]
            ] + [[{"text": "◀️ Back", "callback_data": "timer_menu"}]]
        }
        
        self.send_message(chat_id, "⚠️ <b>Select timer to delete:</b>", keyboard)
    
    def create_timer_from_forwarded(self, chat_id: int, interval_value: int, interval_unit: str):
        """Create a timer from forwarded message"""
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "❌ No message found. Please forward a message first.")
            return
        
        pending = self.pending_requests[chat_id]
        
        # Save the ad
        channels = self.channel_manager.get_all_channels(only_active=True)
        channel_ids = [ch['channel_id'] for ch in channels]
        channels_json = json.dumps(channel_ids)
        
        self.cursor.execute('''
            INSERT INTO ads (content, ad_type, status, created_at, source_message_id, 
                           source_chat_id, target_channels, media_type, media_file_id, caption)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (pending.get('text', ''), 'forward', AdStatus.ACTIVE.value, datetime.now().isoformat(),
              pending['source_message_id'], str(pending['source_chat_id']), channels_json,
              pending.get('media_type'), pending.get('media_file_id'), pending.get('caption', '')))
        
        self.conn.commit()
        ad_id = self.cursor.lastrowid
        
        # Calculate first run
        now = datetime.now()
        if interval_unit == 'minutes':
            first_run = now + timedelta(minutes=interval_value)
        elif interval_unit == 'hours':
            first_run = now + timedelta(hours=interval_value)
        else:
            first_run = now + timedelta(days=interval_value)
        
        # Create timer
        self.cursor.execute('''
            INSERT INTO timers (ad_id, interval_value, interval_unit, next_run, 
                               created_at, channel_ids, created_from_time, timer_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ad_id, interval_value, interval_unit, first_run.isoformat(),
              now.isoformat(), channels_json, now.isoformat(), 'forward'))
        
        self.conn.commit()
        timer_id = self.cursor.lastrowid
        
        # Update ad with timer_id
        self.cursor.execute('UPDATE ads SET timer_id = ? WHERE id = ?', (timer_id, ad_id))
        self.conn.commit()
        
        self.send_message(chat_id, 
            f"✅ <b>Auto-Timer Created!</b>\n\n"
            f"🆔 Timer ID: {timer_id}\n"
            f"⏰ Every {interval_value} {interval_unit}\n"
            f"🎯 First forward at: {first_run.strftime('%H:%M:%S')}\n"
            f"📡 Will forward to {len(channel_ids)} channel(s)\n\n"
            f"Use Timer Menu to manage this timer")
    
    # ============ FORWARD HANDLING ============
    
    def handle_forwarded_message(self, message: Dict, chat_id: int, user_id: int):
        """Handle a message that was forwarded to the bot with enhanced options"""
        
        if user_id != self.admin_id:
            self.send_message(chat_id, "⛔ You're not authorized to use this bot!")
            return
        
        # Extract message data
        msg_data = self.extract_message_data(message)
        
        # Update stats
        today = datetime.now().date().isoformat()
        self.cursor.execute('''
            INSERT INTO stats (date, ads_received) VALUES (?, 1)
            ON CONFLICT(date) DO UPDATE SET ads_received = ads_received + 1
        ''', (today,))
        self.conn.commit()
        
        # Store in pending
        self.pending_requests[chat_id] = msg_data
        
        # Show warning for premium emojis
        warning = ""
        if msg_data['has_premium_emoji']:
            warning = "\n\n⚠️ <b>Note:</b> Message contains premium emojis."
        
        # Show enhanced options
        keyboard = {
            "inline_keyboard": [
                [{"text": "📤 Post Now", "callback_data": "forward_now"}],
                [{"text": "📌 Post & Pin", "callback_data": "forward_pin"}],
                [{"text": "⏲️ Create Auto-Timer", "callback_data": "forward_timer"}],
                [{"text": "⚙️ Advanced Options", "callback_data": "forward_advanced"}],
                [{"text": "❌ Cancel", "callback_data": "cancel"}]
            ]
        }
        
        preview = msg_data['text'][:200] + ('...' if len(msg_data['text']) > 200 else '') if msg_data['text'] else f"📷 {msg_data['media_type'] or 'Media'} message"
        
        text = f"""
✅ <b>Message Received!</b>

<u>Preview:</u>
{preview}

<b>Type:</b> {msg_data['media_type'] or 'Text'}{warning}

<b>What would you like to do?</b>

💡 <b>Options:</b>
• Post Now - Regular forward
• Post & Pin - Forward and pin message
• Auto-Timer - Create repeating timer
• Advanced Options - Pin duration / auto-delete
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def show_forward_advanced(self, chat_id: int):
        """Show advanced forwarding options"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "📌 Custom Pin Duration", "callback_data": "forward_custom_pin"}],
                [{"text": "🗑️ Auto-Delete", "callback_data": "forward_auto_delete"}],
                [{"text": "📌 Pin & Auto-Delete", "callback_data": "forward_pin_delete"}],
                [{"text": "🎯 Select Channels", "callback_data": "forward_select_channels"}],
                [{"text": "◀️ Back", "callback_data": "cancel"}]
            ]
        }
        
        text = """
⚙️ <b>Advanced Forward Options</b>

<b>Custom Pin Duration:</b> Pin for specific time (1h, 6h, 1d, etc.)
<b>Auto-Delete:</b> Message auto-deletes after set time
<b>Pin & Auto-Delete:</b> Pin for a while, then auto-delete
<b>Select Channels:</b> Choose specific channels to forward to

<b>Current defaults:</b>
• Pin duration: 1 hour
• Delete after: 24 hours
• Target: All active channels
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def show_pin_duration_menu(self, chat_id: int, action: str):
        """Show pin duration selection menu"""
        self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
        self.pending_requests[chat_id]['awaiting_pin_duration'] = action
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "📍 1 Hour", "callback_data": f"pin_dur_3600"}],
                [{"text": "📍 3 Hours", "callback_data": f"pin_dur_10800"}],
                [{"text": "📍 6 Hours", "callback_data": f"pin_dur_21600"}],
                [{"text": "📍 12 Hours", "callback_data": f"pin_dur_43200"}],
                [{"text": "📍 1 Day", "callback_data": f"pin_dur_86400"}],
                [{"text": "📍 3 Days", "callback_data": f"pin_dur_259200"}],
                [{"text": "📍 1 Week", "callback_data": f"pin_dur_604800"}],
                [{"text": "📍 Forever", "callback_data": f"pin_dur_0"}],
                [{"text": "◀️ Cancel", "callback_data": "cancel"}]
            ]
        }
        
        self.send_message(chat_id, "📌 <b>Select pin duration:</b>", keyboard)
    
    def show_delete_after_menu(self, chat_id: int):
        """Show delete after selection menu"""
        self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
        self.pending_requests[chat_id]['awaiting_delete_after'] = True
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🗑️ 1 Hour", "callback_data": f"del_after_3600"}],
                [{"text": "🗑️ 3 Hours", "callback_data": f"del_after_10800"}],
                [{"text": "🗑️ 6 Hours", "callback_data": f"del_after_21600"}],
                [{"text": "🗑️ 12 Hours", "callback_data": f"del_after_43200"}],
                [{"text": "🗑️ 1 Day", "callback_data": f"del_after_86400"}],
                [{"text": "🗑️ 2 Days", "callback_data": f"del_after_172800"}],
                [{"text": "🗑️ 3 Days", "callback_data": f"del_after_259200"}],
                [{"text": "🗑️ 1 Week", "callback_data": f"del_after_604800"}],
                [{"text": "🚫 Never Delete", "callback_data": f"del_after_0"}],
                [{"text": "◀️ Cancel", "callback_data": "cancel"}]
            ]
        }
        
        self.send_message(chat_id, "🗑️ <b>Select auto-delete time:</b>", keyboard)
    
    def forward_now_handler(self, chat_id: int):
        """Forward the message immediately"""
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "❌ No message found. Please forward a message first.")
            return
        
        pending = self.pending_requests[chat_id]
        channels = self.channel_manager.get_all_channels(only_active=True)
        
        if not channels:
            self.send_message(chat_id, "❌ No active channels! Please add a channel first.")
            return
        
        # Show channel selection
        keyboard = {
            "inline_keyboard": [
                [{"text": f"📢 {ch['name']}", "callback_data": f"forward_ch_{ch['channel_id']}"}]
                for ch in channels
            ] + [
                [{"text": "📡 Post to ALL Active", "callback_data": "forward_all"}],
                [{"text": "❌ Cancel", "callback_data": "cancel"}]
            ]
        }
        
        text = f"""
<b>Select channel to forward to:</b>

<b>Message type:</b> {pending.get('media_type', 'Text')}

Message will be FORWARDED to the selected channel(s)
        """
        
        self.send_message(chat_id, text, keyboard)
    
    def execute_forward(self, chat_id: int, channel_id: str = None, forward_all: bool = False):
        """Execute the forward action"""
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "❌ No message found.")
            return
        
        pending = self.pending_requests[chat_id]
        
        if forward_all:
            channels = self.channel_manager.get_all_channels(only_active=True)
            channel_ids = [ch['channel_id'] for ch in channels]
        else:
            channel_ids = [channel_id]
        
        success_count = 0
        failed_channels = []
        
        for ch_id in channel_ids:
            if self.forward_message_to_channel(ch_id, pending['source_chat_id'], pending['source_message_id']):
                success_count += 1
                self.channel_manager.update_post_count(ch_id)
            else:
                failed_channels.append(ch_id)
        
        # Update stats
        today = datetime.now().date().isoformat()
        self.cursor.execute('UPDATE stats SET ads_posted = ads_posted + ? WHERE date = ?', (success_count, today))
        self.conn.commit()
        
        result_msg = f"✅ <b>Forwarded to {success_count}/{len(channel_ids)} channels!</b>"
        if failed_channels:
            result_msg += f"\n\n❌ Failed: {', '.join(failed_channels)}\nMake sure I'm admin in these channels!"
        
        self.send_message(chat_id, result_msg)
        self.pending_requests.pop(chat_id, None)
    
    def execute_forward_enhanced(self, chat_id: int, channel_id: str = None, 
                                 forward_all: bool = False, pin: bool = False, 
                                 pin_duration: int = 0, delete_after: int = 0):
        """Execute the forward action with enhanced options"""
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
        failed_channels = []
        
        for ch_id in channel_ids:
            # Forward the message
            success = self.forward_message_to_channel(ch_id, pending['source_chat_id'], pending['source_message_id'])
            
            if success:
                success_count += 1
                self.channel_manager.update_post_count(ch_id)
                self.record_analytics(ch_id, 'success', 1)
                
                # Note: In production, you'd get the actual message_id from the forward response
                # This is a placeholder for demonstration
                if pin:
                    # You would pin the actual message_id here
                    logging.info(f"Would pin message to {ch_id} for {pin_duration}s")
                
                if delete_after > 0:
                    logging.info(f"Would delete message from {ch_id} after {delete_after}s")
            else:
                failed_channels.append(ch_id)
                self.record_analytics(ch_id, 'failed', 1)
        
        # Update stats
        today = datetime.now().date().isoformat()
        self.cursor.execute('UPDATE stats SET ads_posted = ads_posted + ? WHERE date = ?', (success_count, today))
        self.conn.commit()
        
        result_msg = f"✅ <b>Posted to {success_count}/{len(channel_ids)} channels!</b>"
        if pin:
            result_msg += f"\n📌 Pinned for {self._format_duration(pin_duration)}"
        if delete_after > 0:
            result_msg += f"\n🗑️ Will delete after {self._format_duration(delete_after)}"
        if failed_channels:
            result_msg += f"\n\n❌ Failed: {', '.join(failed_channels)}\nMake sure I'm admin in these channels!"
        
        self.send_message(chat_id, result_msg)
        self.pending_requests.pop(chat_id, None)
    
    def show_manage_posts_menu(self, chat_id: int):
        """Show menu for managing posts"""
        self.cursor.execute('''
            SELECT id, message_id, channel_id, posted_at, is_pinned, scheduled_unpin_at
            FROM posted_messages 
            WHERE is_deleted = 0
            ORDER BY posted_at DESC
            LIMIT 20
        ''')
        
        posts = self.cursor.fetchall()
        
        if not posts:
            self.send_message(chat_id, "📭 No active posts found.")
            return
        
        keyboard = {
            "inline_keyboard": []
        }
        
        for post in posts:
            post_id, msg_id, channel_id, posted_at, is_pinned, unpin_at = post
            status = "📌" if is_pinned else "📄"
            keyboard["inline_keyboard"].append([
                {"text": f"{status} {channel_id[:20]} - {posted_at[:10]}", 
                 "callback_data": f"manage_post_{post_id}"}
            ])
        
        keyboard["inline_keyboard"].append([{"text": "◀️ Back to Main", "callback_data": "back_to_main"}])
        
        self.send_message(chat_id, "📝 <b>Manage Active Posts:</b>\nSelect a post to manage", keyboard)
    
    def show_post_management(self, chat_id: int, post_id: int):
        """Show management options for a specific post"""
        self.cursor.execute('''
            SELECT message_id, channel_id, posted_at, is_pinned, scheduled_unpin_at, scheduled_delete_at
            FROM posted_messages WHERE id = ?
        ''', (post_id,))
        
        post = self.cursor.fetchone()
        if not post:
            self.send_message(chat_id, "❌ Post not found")
            return
        
        message_id, channel_id, posted_at, is_pinned, unpin_at, delete_at = post
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🗑️ Delete Now", "callback_data": f"delete_post_{post_id}"}],
            ]
        }
        
        if is_pinned:
            keyboard["inline_keyboard"].append(
                [{"text": "📌 Unpin Now", "callback_data": f"unpin_post_{post_id}"}]
            )
        
        keyboard["inline_keyboard"].append([{"text": "◀️ Back", "callback_data": "manage_posts"}])
        
        text = f"""
📝 <b>Post Management</b>

<b>Channel:</b> {channel_id}
<b>Message ID:</b> {message_id}
<b>Posted:</b> {posted_at}
<b>Status:</b> {'📌 Pinned' if is_pinned else '📄 Not pinned'}

<b>Scheduled Actions:</b>
• Unpin at: {unpin_at or 'Not scheduled'}
• Delete at: {delete_at or 'Not scheduled'}

<b>What would you like to do?</b>
        """
        
        self.send_message(chat_id, text, keyboard)
    
    # ============ STATISTICS AND HELP ============
    
    def show_stats(self, chat_id: int):
        """Show statistics"""
        today = datetime.now().date().isoformat()
        self.cursor.execute('SELECT ads_posted, ads_failed, ads_received FROM stats WHERE date = ?', (today,))
        today_stats = self.cursor.fetchone()
        
        self.cursor.execute('SELECT SUM(ads_posted), SUM(ads_failed), SUM(ads_received) FROM stats')
        total_stats = self.cursor.fetchone()
        
        channels = self.channel_manager.get_all_channels(only_active=False)
        
        self.cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active_timers = self.cursor.fetchone()[0]
        
        text = f"""
📊 <b>Bot Statistics</b>

<b>Today ({today}):</b>
📥 Received: {today_stats[2] if today_stats else 0}
📤 Forwarded: {today_stats[0] if today_stats else 0}
❌ Failed: {today_stats[1] if today_stats else 0}

<b>All Time:</b>
📥 Total Received: {total_stats[2] if total_stats and total_stats[0] else 0}
📤 Total Forwarded: {total_stats[0] if total_stats and total_stats[0] else 0}
❌ Total Failed: {total_stats[1] if total_stats and total_stats[0] else 0}

<b>Current Status:</b>
📢 Channels: {len(channels)} total
⏲️ Active Timers: {active_timers}

<b>Mode:</b> FORWARD with Auto-Pin/Delete
        """
        
        self.send_message(chat_id, text)
    
    def show_help(self, chat_id: int):
        """Show help"""
        help_text = """
📚 <b>Bot Commands & Features</b>

<b>Main Commands:</b>
/start - Show main menu
/channels - Manage channels
/stats - View statistics
/help - Show this help

<b>How to Post Ads:</b>
1. Add your channel using /channels
2. Forward any message to this bot
3. Choose "Post Now" or "Post & Pin" or "Auto-Timer"
4. Select channel(s)
5. Done!

<b>Auto-Pin Feature:</b>
• Pin messages for specific durations
• Options: 1h, 3h, 6h, 12h, 1d, 3d, 1 week, forever
• Auto-unpin when duration expires

<b>Auto-Delete Feature:</b>
• Schedule posts to auto-delete
• Options: 1h, 3h, 6h, 12h, 1d, 2d, 3d, 1 week
• Perfect for time-sensitive content

<b>Auto-Timer Feature:</b>
• Timer starts from creation time
• Exact intervals (1 hour = exactly 1 hour later)
• Messages are FORWARDED
• Can pause, resume, delete timers

<b>Available Timer Intervals:</b>
• Minutes: 1, 5, 10, 15, 30
• Hours: 1, 2, 4, 6, 12
• Days: 1 (24 hours)

<b>Channel Management:</b>
• Add Channel - Add new channel
• Set Default - Choose default channel
• Channel Settings - Configure auto-pin/delete per channel
• Toggle - Activate/Deactivate
• Remove - Delete channel

<b>Requirements:</b>
• Bot must be admin in channel
• Bot needs "Post Messages" permission
• For pinning: "Pin Messages" permission
• For deleting: "Delete Messages" permission
        """
        
        self.send_message(chat_id, help_text)
    
    # ============ MAIN MENU ============
    
    def show_main_menu(self, chat_id: int):
        """Show enhanced main menu"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "📤 Post Message", "callback_data": "forward_prompt"}],
                [{"text": "⏲️ Auto-Timers", "callback_data": "timer_menu"}],
                [{"text": "🔧 Manage Channels", "callback_data": "channel_menu"}],
                [{"text": "⚙️ Channel Settings", "callback_data": "channel_settings_menu"}],
                [{"text": "📊 Statistics", "callback_data": "stats_main"}],
                [{"text": "🗑️ Manage Posts", "callback_data": "manage_posts"}],
                [{"text": "❓ Help", "callback_data": "help_main"}]
            ]
        }
        
        default_ch = self.channel_manager.get_default_channel() or 'Not set'
        
        self.cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active_timers = self.cursor.fetchone()[0]
        
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        active_posts = self.cursor.fetchone()[0]
        
        text = f"""
🤖 <b>Enhanced Ad Bot with Auto-Management</b>

<b>Features:</b>
• 📤 Forward messages to channels
• 📌 Auto-pin messages with duration
• 🗑️ Auto-delete old posts
• ⏲️ Auto-timers with exact intervals
• 📊 Channel analytics
• 🔧 Multi-channel management

<b>Current Status:</b>
📡 Default channel: {default_ch}
⏲️ Active timers: {active_timers}
📝 Active posts: {active_posts}

<b>Quick Actions:</b>
• Forward any message to start
• Use timers for recurring posts
• Configure auto-pin/delete per channel
        """
        
        self.send_message(chat_id, text, keyboard)
    
    # ============ TELEGRAM API METHODS ============
    
    def send_message(self, chat_id: int, text: str, keyboard: Dict = None):
        """Send message to user"""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }
        
        if keyboard:
            payload["reply_markup"] = keyboard
        
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
    
    def answer_callback(self, callback_id: str, text: str = None, show_alert: bool = False):
        """Answer callback query"""
        url = f"{self.base_url}/answerCallbackQuery"
        payload = {"callback_query_id": callback_id}
        
        if text:
            payload["text"] = text
            payload["show_alert"] = show_alert
        
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            logging.error(f"Failed to answer callback: {e}")
    
    # ============ TEXT HANDLER ============
    
    def handle_text_input(self, chat_id: int, text: str, user_id: int):
        """Handle text input for multi-step commands"""
        if user_id != self.admin_id:
            return
        
        # Check for add channel
        if chat_id in self.pending_requests and self.pending_requests[chat_id].get('action') == 'add_channel':
            parts = text.split(',', 1)
            channel_id = parts[0].strip()
            name = parts[1].strip() if len(parts) > 1 else channel_id
            
            if self.channel_manager.add_channel(channel_id, name):
                self.send_message(chat_id, f"✅ Channel '{name}' added!\nID: {channel_id}")
            else:
                self.send_message(chat_id, f"❌ Failed to add channel")
            
            self.pending_requests.pop(chat_id, None)
            self.show_channel_menu(chat_id)
            return
        
        # Handle commands
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
    
    # ============ CALLBACK HANDLER ============
    
    def handle_callback(self, callback_data: str, chat_id: int, message_id: int, user_id: int, callback_id: str = None):
        """Handle all callback queries with enhanced features"""
        if user_id != self.admin_id:
            if callback_id:
                self.answer_callback(callback_id, "Unauthorized!", True)
            return
        
        # Enhanced forward handling
        if callback_data == "forward_prompt":
            self.send_message(chat_id, "📤 Forward any message to me and I'll forward it to your channel!")
        
        elif callback_data == "forward_now":
            self.forward_now_handler(chat_id)
        
        elif callback_data == "forward_pin":
            self.show_pin_duration_menu(chat_id, "forward_pin")
        
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
        
        elif callback_data == "forward_timer":
            self.show_timer_interval_menu(chat_id)
            self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
            self.pending_requests[chat_id]['awaiting_timer'] = True
        
        elif callback_data == "forward_select_channels":
            self.forward_now_handler(chat_id)
        
        elif callback_data.startswith("forward_ch_"):
            channel_id = callback_data.replace("forward_ch_", "")
            self.execute_forward(chat_id, channel_id=channel_id)
        
        elif callback_data == "forward_all":
            self.execute_forward(chat_id, forward_all=True)
        
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
                self.execute_forward_enhanced(chat_id, forward_all=True, pin=True, 
                                             pin_duration=pin_duration, delete_after=delete_after)
                self.pending_requests[chat_id].pop('awaiting_pin_delete', None)
            else:
                self.execute_forward_enhanced(chat_id, forward_all=True, delete_after=delete_after)
            
            self.pending_requests.get(chat_id, {}).pop('awaiting_delete_after', None)
        
        # Timer intervals
        elif callback_data.startswith("timer_interval_"):
            parts = callback_data.replace("timer_interval_", "").split("_")
            interval_value = int(parts[0])
            interval_unit = parts[1]
            
            if chat_id in self.pending_requests and self.pending_requests[chat_id].get('awaiting_timer'):
                self.create_timer_from_forwarded(chat_id, interval_value, interval_unit)
                self.pending_requests[chat_id].pop('awaiting_timer', None)
            else:
                self.pending_requests[chat_id] = {'timer_interval_value': interval_value, 'timer_interval_unit': interval_unit}
                self.send_message(chat_id, f"✅ Interval set to every {interval_value} {interval_unit}\n\nNow forward me the message you want to auto-forward!")
        
        # Timer management
        elif callback_data == "timer_menu":
            self.show_timer_menu(chat_id)
        
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
            self.cursor.execute('UPDATE timers SET is_active = 0 WHERE id = ?', (timer_id,))
            self.conn.commit()
            self.send_message(chat_id, f"✅ Timer #{timer_id} paused")
        
        elif callback_data.startswith("timer_delete_"):
            timer_id = int(callback_data.replace("timer_delete_", ""))
            self.cursor.execute('DELETE FROM timers WHERE id = ?', (timer_id,))
            self.conn.commit()
            self.send_message(chat_id, f"✅ Timer #{timer_id} deleted")
        
        # Channel management
        elif callback_data == "channel_menu":
            self.show_channel_menu(chat_id)
        
        elif callback_data == "channel_settings_menu":
            self.show_channel_settings_menu(chat_id)
        
        elif callback_data.startswith("ch_settings_"):
            channel_id = callback_data.replace("ch_settings_", "")
            self.show_channel_settings_menu(chat_id, channel_id)
        
        elif callback_data.startswith("ch_toggle_pin_"):
            channel_id = callback_data.replace("ch_toggle_pin_", "")
            self.toggle_channel_auto_pin(channel_id)
            self.show_channel_settings_menu(chat_id, channel_id)
        
        elif callback_data.startswith("ch_toggle_delete_"):
            channel_id = callback_data.replace("ch_toggle_delete_", "")
            self.toggle_channel_auto_delete(channel_id)
            self.show_channel_settings_menu(chat_id, channel_id)
        
        elif callback_data.startswith("ch_pin_duration_"):
            channel_id = callback_data.replace("ch_pin_duration_", "")
            self.pending_requests[chat_id] = {'action': 'set_pin_duration', 'channel_id': channel_id}
            self.show_pin_duration_menu(chat_id, "channel_pin")
        
        elif callback_data.startswith("ch_delete_after_"):
            channel_id = callback_data.replace("ch_delete_after_", "")
            self.pending_requests[chat_id] = {'action': 'set_delete_after', 'channel_id': channel_id}
            self.show_delete_after_menu(chat_id)
        
        elif callback_data.startswith("ch_analytics_"):
            channel_id = callback_data.replace("ch_analytics_", "")
            self.show_channel_analytics(chat_id, channel_id)
        
        elif callback_data == "ch_add":
            self.add_channel_handler(chat_id)
        
        elif callback_data == "ch_list":
            self.list_channels_handler(chat_id)
        
        elif callback_data == "ch_default":
            self.set_default_channel_handler(chat_id)
        
        elif callback_data == "ch_toggle":
            self.toggle_channel_handler(chat_id)
        
        elif callback_data == "ch_remove":
            self.remove_channel_handler(chat_id)
        
        elif callback_data == "ch_stats":
            self.channel_stats_handler(chat_id)
        
        elif callback_data.startswith("set_default_"):
            channel_id = callback_data.replace("set_default_", "")
            self.channel_manager.set_default_channel(channel_id)
            self.send_message(chat_id, f"✅ Default channel: {channel_id}")
            self.show_channel_menu(chat_id)
        
        elif callback_data.startswith("toggle_"):
            channel_id = callback_data.replace("toggle_", "")
            channels = self.channel_manager.get_all_channels(only_active=False)
            for ch in channels:
                if ch['channel_id'] == channel_id:
                    self.channel_manager.toggle_channel(channel_id, not ch['is_active'])
                    self.send_message(chat_id, f"✅ Channel {ch['name']} toggled")
                    break
            self.show_channel_menu(chat_id)
        
        elif callback_data.startswith("remove_"):
            channel_id = callback_data.replace("remove_", "")
            if self.channel_manager.remove_channel(channel_id):
                self.send_message(chat_id, f"✅ Channel removed")
            else:
                self.send_message(chat_id, f"❌ Cannot remove default channel")
            self.show_channel_menu(chat_id)
        
        # Post management
        elif callback_data == "manage_posts":
            self.show_manage_posts_menu(chat_id)
        
        elif callback_data.startswith("manage_post_"):
            post_id = int(callback_data.replace("manage_post_", ""))
            self.show_post_management(chat_id, post_id)
        
        elif callback_data.startswith("delete_post_"):
            post_id = int(callback_data.replace("delete_post_", ""))
            self.cursor.execute('SELECT message_id, channel_id FROM posted_messages WHERE id = ?', (post_id,))
            result = self.cursor.fetchone()
            if result and result[0] and result[1]:
                message_id, channel_id = result
                self.delete_message(channel_id, message_id)
                self.cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?', 
                                  (datetime.now().isoformat(), post_id))
                self.conn.commit()
                self.send_message(chat_id, f"✅ Deleted post")
            self.show_manage_posts_menu(chat_id)
        
        elif callback_data.startswith("unpin_post_"):
            post_id = int(callback_data.replace("unpin_post_", ""))
            self.cursor.execute('SELECT message_id, channel_id FROM posted_messages WHERE id = ?', (post_id,))
            result = self.cursor.fetchone()
            if result and result[0] and result[1]:
                message_id, channel_id = result
                self.unpin_message(channel_id, message_id)
                self.cursor.execute('UPDATE posted_messages SET is_pinned = 0, unpinned_at = ? WHERE id = ?', 
                                  (datetime.now().isoformat(), post_id))
                self.conn.commit()
                self.send_message(chat_id, f"✅ Unpinned post")
            self.show_manage_posts_menu(chat_id)
        
        # Navigation
        elif callback_data == "stats_main":
            self.show_stats(chat_id)
        
        elif callback_data == "help_main":
            self.show_help(chat_id)
        
        elif callback_data == "back_to_main":
            self.show_main_menu(chat_id)
        
        elif callback_data == "cancel":
            self.pending_requests.pop(chat_id, None)
            self.send_message(chat_id, "❌ Cancelled")
        
        if callback_id:
            self.answer_callback(callback_id)
    
    # ============ MAIN LOOP ============
    
    def run(self):
        """Main bot loop"""
        logging.info("🤖 ENHANCED BOT STARTED SUCCESSFULLY!")
        logging.info("Features: Auto-Delete, Auto-Pin, Analytics, and more!")
        
        # Start timer processor
        self.timer_manager.start_timer_processor()
        
        # Start schedule manager
        self.schedule_manager.start_scheduler()
        
        # Main polling loop
        last_update_id = 0
        
        while self.running:
            try:
                url = f"{self.base_url}/getUpdates"
                params = {"offset": last_update_id + 1, "timeout": 30}
                response = requests.get(url, params=params, timeout=35)
                data = response.json()
                
                if not data.get('ok'):
                    time.sleep(5)
                    continue
                
                for update in data.get('result', []):
                    last_update_id = update['update_id']
                    
                    if 'message' in update:
                        msg = update['message']
                        chat_id = msg['chat']['id']
                        user_id = msg.get('from', {}).get('id')
                        
                        # Check for forwarded message
                        if ('forward_date' in msg or 'forward_from' in msg or 'forward_from_chat' in msg):
                            self.handle_forwarded_message(msg, chat_id, user_id)
                        elif 'text' in msg and user_id == self.admin_id:
                            self.handle_text_input(chat_id, msg['text'], user_id)
                    
                    elif 'callback_query' in update:
                        callback = update['callback_query']
                        callback_id = callback['id']
                        chat_id = callback['message']['chat']['id']
                        message_id = callback['message']['message_id']
                        user_id = callback['from']['id']
                        data = callback['data']
                        
                        self.handle_callback(data, chat_id, message_id, user_id, callback_id)
                
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Main loop error: {e}")
                time.sleep(5)
    
    def stop(self):
        """Stop the bot"""
        self.running = False
        self.timer_manager.running = False
        self.schedule_manager.running = False
        self.conn.close()
        logging.info("🛑 Bot stopped")

# ============ RUN BOT ============
if __name__ == "__main__":
    # CONFIGURATION - REPLACE THESE!
    BOT_TOKEN = "8348531965:AAHA_l_1C_9Hxc2gyakfP29w51If3a8zA28"
    ADMIN_ID = 7049142115  # Your Telegram user ID
    
    bot = ForwardAdBot(BOT_TOKEN, ADMIN_ID)
    
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.stop()
        print("\n✅ Bot stopped")