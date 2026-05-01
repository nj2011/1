import requests
import sqlite3
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum

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
            if len(timer) < 20:
                return
            
            timer_id = timer[0]
            ad_id = timer[1]
            channels_json = timer[10] if len(timer) > 10 else None
            source_chat_id = timer[18] if len(timer) > 18 else None
            source_message_id = timer[19] if len(timer) > 19 else None
            pin = timer[20] if len(timer) > 20 else False
            pin_duration = timer[21] if len(timer) > 21 else 3600
            delete_after = timer[22] if len(timer) > 22 else 0
            
            logging.info(f"⏰ Executing timer #{timer_id}")
            
            channels = json.loads(channels_json) if channels_json else []
            if not channels:
                default_ch = self.bot.channel_manager.get_default_channel()
                if default_ch:
                    channels = [default_ch]
            
            success_count = 0
            for channel in channels:
                if channel and source_chat_id and source_message_id:
                    success, new_message_id = self.bot.forward_message_to_channel_with_response(
                        channel, str(source_chat_id), source_message_id
                    )
                    
                    if success and new_message_id:
                        success_count += 1
                        self.bot.channel_manager.update_post_count(channel)
                        
                        if pin and pin_duration > 0:
                            self.bot.pin_message(channel, new_message_id, pin_duration)
                        
                        if delete_after > 0:
                            self.bot.schedule_message_deletion(channel, new_message_id, ad_id, delete_after)
                        
                        self.bot.record_posted_message(new_message_id, channel, ad_id, pin, pin_duration, delete_after)
            
            total_runs = timer[6] if len(timer) > 6 else 0
            max_runs = timer[7] if len(timer) > 7 else None
            new_total = (total_runs or 0) + 1
            is_active = 0 if max_runs and new_total >= max_runs else 1
            
            self.bot.cursor.execute('''
                UPDATE timers SET last_run = ?, total_runs = ?, is_active = ? WHERE id = ?
            ''', (datetime.now().isoformat(), new_total, is_active, timer_id))
            self.bot.conn.commit()
            
            logging.info(f"✅ Timer #{timer_id} posted to {success_count}/{len(channels)} channels")
            
            if is_active:
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
            
            self.bot.cursor.execute('''
                UPDATE timers SET next_run = ?, is_active = ? WHERE id = ?
            ''', (next_time.isoformat(), is_active, timer_id))
            self.bot.conn.commit()
            
        except Exception as e:
            logging.error(f"Error updating timer schedule: {e}")

class ChannelManager:
    """Manage multiple channels"""
    
    def __init__(self, db_connection):
        self.conn = db_connection
        self.cursor = db_connection.cursor()
        self.init_channel_table()
    
    def init_channel_table(self):
        """Initialize channels table"""
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
                default_delete_after INTEGER DEFAULT 86400
            )
        ''')
        
        self.cursor.execute("PRAGMA table_info(channels)")
        existing_columns = [col[1] for col in self.cursor.fetchall()]
        
        new_columns = {
            'auto_pin': 'BOOLEAN DEFAULT 0',
            'auto_delete': 'BOOLEAN DEFAULT 0',
            'default_pin_duration': 'INTEGER DEFAULT 3600',
            'default_delete_after': 'INTEGER DEFAULT 86400'
        }
        
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                try:
                    self.cursor.execute(f"ALTER TABLE channels ADD COLUMN {col_name} {col_type}")
                except Exception as e:
                    pass
        
        self.cursor.execute('SELECT COUNT(*) FROM channels WHERE is_default = 1')
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute('''
                INSERT INTO channels (channel_id, name, is_default, created_at)
                VALUES ('@default', 'Default Channel', 1, ?)
            ''', (datetime.now().isoformat(),))
            self.conn.commit()
        
        self.conn.commit()
    
    def add_channel(self, channel_id: str, name: str = None, description: str = None) -> bool:
        try:
            self.cursor.execute('''
                INSERT INTO channels (channel_id, name, description, created_at)
                VALUES (?, ?, ?, ?)
            ''', (channel_id, name or channel_id, description, datetime.now().isoformat()))
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
                self.send_message(chat_id, "❌ Cannot remove default channel")
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
    
    def get_all_channels(self, only_active: bool = True) -> List[Dict]:
        query = 'SELECT channel_id, name, is_active, is_default, description, post_count FROM channels'
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
                'post_count': row[5]
            })
        return channels
    
    def get_default_channel(self) -> str:
        self.cursor.execute('SELECT channel_id FROM channels WHERE is_default = 1 AND is_active = 1')
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def update_post_count(self, channel_id: str):
        self.cursor.execute('UPDATE channels SET post_count = post_count + 1 WHERE channel_id = ?', (channel_id,))
        self.conn.commit()

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
    
    def init_database(self):
        """Initialize SQLite database"""
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
                delete_after INTEGER DEFAULT 0
            )
        ''')
        
        # Posted messages table
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
                unpinned_at TEXT
            )
        ''')
        
        # Timers table
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
        
        # Statistics table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                date TEXT PRIMARY KEY,
                ads_posted INTEGER DEFAULT 0,
                ads_failed INTEGER DEFAULT 0,
                ads_received INTEGER DEFAULT 0,
                messages_deleted INTEGER DEFAULT 0
            )
        ''')
        
        self.conn.commit()
        logging.info("Database initialized")
    
    # ============ TELEGRAM API METHODS ============
    
    def forward_message_to_channel_with_response(self, channel_id: str, from_chat_id: str, message_id: int) -> Tuple[bool, Optional[int]]:
        """Forward a message and return the new message ID"""
        if not from_chat_id or not message_id:
            return False, None
            
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
                    result = data.get('result', {})
                    new_message_id = result.get('message_id')
                    return True, new_message_id
            return False, None
        except Exception as e:
            logging.warning(f"Forward exception: {e}")
            return False, None
    
    def forward_message_to_channel(self, channel_id: str, from_chat_id: str, message_id: int) -> bool:
        success, _ = self.forward_message_to_channel_with_response(channel_id, from_chat_id, message_id)
        return success
    
    def pin_message(self, channel_id: str, message_id: int, duration: int = 0) -> bool:
        """Pin a message to channel"""
        url = f"{self.base_url}/pinChatMessage"
        payload = {
            "chat_id": channel_id,
            "message_id": message_id,
            "disable_notification": True
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                logging.info(f"📌 Pinned message {message_id} to {channel_id}")
                if duration > 0:
                    self.schedule_message_unpin(channel_id, message_id, duration)
                return True
            return False
        except Exception as e:
            logging.warning(f"Pin exception: {e}")
            return False
    
    def unpin_message(self, channel_id: str, message_id: int = None) -> bool:
        """Unpin message from channel"""
        url = f"{self.base_url}/unpinChatMessage"
        payload = {"chat_id": channel_id}
        if message_id:
            payload["message_id"] = message_id
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
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
                logging.info(f"🗑️ Deleted message {message_id}")
                return True
            return False
        except Exception as e:
            logging.warning(f"Delete exception: {e}")
            return False
    
    def schedule_message_unpin(self, channel_id: str, message_id: int, duration_seconds: int):
        """Schedule a message to be unpinned"""
        def unpin_task():
            time.sleep(duration_seconds)
            self.unpin_message(channel_id, message_id)
            self.cursor.execute('''
                UPDATE posted_messages SET is_pinned = 0, unpinned_at = ?
                WHERE channel_id = ? AND message_id = ?
            ''', (datetime.now().isoformat(), channel_id, message_id))
            self.conn.commit()
        
        threading.Thread(target=unpin_task, daemon=True).start()
        logging.info(f"⏰ Scheduled unpin in {duration_seconds}s")
    
    def schedule_message_deletion(self, channel_id: str, message_id: int, ad_id: int, delete_after_seconds: int):
        """Schedule a message for deletion"""
        def delete_task():
            time.sleep(delete_after_seconds)
            self.delete_message(channel_id, message_id)
            self.cursor.execute('''
                UPDATE posted_messages SET is_deleted = 1, deleted_at = ?
                WHERE channel_id = ? AND message_id = ? AND ad_id = ?
            ''', (datetime.now().isoformat(), channel_id, message_id, ad_id))
            self.conn.commit()
        
        threading.Thread(target=delete_task, daemon=True).start()
        logging.info(f"⏰ Scheduled deletion in {delete_after_seconds}s")
    
    def record_posted_message(self, message_id: int, channel_id: str, ad_id: int, pin: bool = False, pin_duration: int = 0, delete_after: int = 0):
        """Record a posted message"""
        self.cursor.execute('''
            INSERT INTO posted_messages (message_id, channel_id, ad_id, posted_at, is_pinned)
            VALUES (?, ?, ?, ?, ?)
        ''', (message_id, channel_id, ad_id, datetime.now().isoformat(), 1 if pin else 0))
        self.conn.commit()
    
    # ============ DELETE POSTS METHODS ============
    
    def show_delete_posts_menu(self, chat_id: int):
        """Show delete posts menu"""
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        total_active = self.cursor.fetchone()[0]
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "🗑️ Delete Single Post", "callback_data": "delete_single_post"}],
                [{"text": "📊 Delete by Channel", "callback_data": "delete_by_channel"}],
                [{"text": "📌 Delete Pinned Posts", "callback_data": "delete_pinned_posts"}],
                [{"text": "⏲️ Delete Expired Posts", "callback_data": "delete_expired_posts"}],
                [{"text": "🗑️ Delete ALL Posts", "callback_data": "delete_all_posts_confirm"}],
                [{"text": "📋 List All Posts", "callback_data": "list_all_posts"}],
                [{"text": "◀️ Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        
        text = f"""
🗑️ <b>Delete Posts Management</b>

<b>Active Posts:</b> {total_active}

Choose an option:
• Delete Single Post - Select from list
• Delete by Channel - Remove all from a channel
• Delete Pinned Posts - Remove all pinned messages
• Delete Expired Posts - Remove old posts
• Delete ALL Posts - ⚠️ Remove everything
        """
        self.send_message(chat_id, text, keyboard)
    
    def list_all_posts(self, chat_id: int, page: int = 0):
        """List all active posts"""
        posts_per_page = 10
        offset = page * posts_per_page
        
        self.cursor.execute('''
            SELECT id, message_id, channel_id, posted_at, is_pinned
            FROM posted_messages WHERE is_deleted = 0
            ORDER BY posted_at DESC LIMIT ? OFFSET ?
        ''', (posts_per_page, offset))
        
        posts = self.cursor.fetchall()
        
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        total = self.cursor.fetchone()[0]
        
        if not posts:
            self.send_message(chat_id, "📭 No active posts")
            return
        
        text = f"📋 <b>Active Posts (Page {page + 1})</b>\n\n"
        for post in posts:
            post_id, msg_id, channel_id, posted_at, is_pinned = post
            pin_icon = "📌" if is_pinned else "📄"
            text += f"{pin_icon} <b>ID: {post_id}</b> | {channel_id}\n"
            text += f"   Msg: {msg_id} | Posted: {posted_at[:16]}\n"
            text += "─" * 25 + "\n"
        
        keyboard = {"inline_keyboard": []}
        nav_buttons = []
        if page > 0:
            nav_buttons.append({"text": "◀️ Prev", "callback_data": f"list_posts_page_{page - 1}"})
        if (page + 1) * posts_per_page < total:
            nav_buttons.append({"text": "Next ▶️", "callback_data": f"list_posts_page_{page + 1}"})
        if nav_buttons:
            keyboard["inline_keyboard"].append(nav_buttons)
        
        keyboard["inline_keyboard"].append([
            {"text": "🗑️ Delete Selected", "callback_data": "delete_single_post"},
            {"text": "◀️ Back", "callback_data": "delete_posts_menu"}
        ])
        
        self.send_message(chat_id, text, keyboard)
    
    def show_delete_by_channel(self, chat_id: int):
        """Show channels for deletion"""
        channels = self.channel_manager.get_all_channels(only_active=False)
        keyboard = {"inline_keyboard": []}
        
        for ch in channels:
            self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE channel_id = ? AND is_deleted = 0', (ch['channel_id'],))
            count = self.cursor.fetchone()[0]
            if count > 0:
                keyboard["inline_keyboard"].append([
                    {"text": f"📢 {ch['name']} ({count} posts)", "callback_data": f"delete_channel_{ch['channel_id']}"}
                ])
        
        if not keyboard["inline_keyboard"]:
            self.send_message(chat_id, "📭 No posts found")
            return
        
        keyboard["inline_keyboard"].append([{"text": "◀️ Back", "callback_data": "delete_posts_menu"}])
        self.send_message(chat_id, "🗑️ <b>Select channel to delete posts from:</b>", keyboard)
    
    def confirm_delete_channel(self, chat_id: int, channel_id: str):
        """Confirm channel deletion"""
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE channel_id = ? AND is_deleted = 0', (channel_id,))
        count = self.cursor.fetchone()[0]
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "✅ YES, Delete All", "callback_data": f"confirm_del_channel_{channel_id}"}],
                [{"text": "❌ Cancel", "callback_data": "delete_posts_menu"}]
            ]
        }
        
        self.send_message(chat_id, f"⚠️ Delete {count} posts from {channel_id}? This cannot be undone!", keyboard)
    
    def execute_delete_channel(self, chat_id: int, channel_id: str):
        """Execute channel deletion"""
        self.cursor.execute('SELECT id, message_id, channel_id FROM posted_messages WHERE channel_id = ? AND is_deleted = 0', (channel_id,))
        posts = self.cursor.fetchall()
        
        deleted = 0
        for post_id, msg_id, ch_id in posts:
            if msg_id and self.delete_message(ch_id, msg_id):
                self.cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?', (datetime.now().isoformat(), post_id))
                deleted += 1
        
        self.conn.commit()
        self.send_message(chat_id, f"✅ Deleted {deleted} posts from {channel_id}")
        self.show_delete_posts_menu(chat_id)
    
    def show_delete_pinned_posts(self, chat_id: int):
        """Confirm delete pinned posts"""
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_pinned = 1 AND is_deleted = 0')
        count = self.cursor.fetchone()[0]
        
        if count == 0:
            self.send_message(chat_id, "📭 No pinned posts")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "✅ YES, Delete All Pinned", "callback_data": "confirm_delete_pinned"}],
                [{"text": "❌ Cancel", "callback_data": "delete_posts_menu"}]
            ]
        }
        
        self.send_message(chat_id, f"⚠️ Delete {count} pinned posts? This will remove them from channels!", keyboard)
    
    def execute_delete_pinned(self, chat_id: int):
        """Execute delete pinned posts"""
        self.cursor.execute('SELECT id, message_id, channel_id FROM posted_messages WHERE is_pinned = 1 AND is_deleted = 0')
        posts = self.cursor.fetchall()
        
        deleted = 0
        for post_id, msg_id, channel_id in posts:
            if msg_id:
                self.unpin_message(channel_id, msg_id)
                if self.delete_message(channel_id, msg_id):
                    self.cursor.execute('UPDATE posted_messages SET is_deleted = 1, is_pinned = 0, deleted_at = ? WHERE id = ?', (datetime.now().isoformat(), post_id))
                    deleted += 1
        
        self.conn.commit()
        self.send_message(chat_id, f"✅ Deleted {deleted} pinned posts")
        self.show_delete_posts_menu(chat_id)
    
    def show_delete_expired_posts(self, chat_id: int):
        """Show expired posts options"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "🗑️ Older than 7 days", "callback_data": "delete_expired_7"}],
                [{"text": "🗑️ Older than 30 days", "callback_data": "delete_expired_30"}],
                [{"text": "🗑️ Older than 60 days", "callback_data": "delete_expired_60"}],
                [{"text": "◀️ Back", "callback_data": "delete_posts_menu"}]
            ]
        }
        self.send_message(chat_id, "⏲️ <b>Delete posts older than:</b>", keyboard)
    
    def execute_delete_expired(self, chat_id: int, days: int):
        """Delete expired posts"""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        self.cursor.execute('SELECT id, message_id, channel_id FROM posted_messages WHERE posted_at < ? AND is_deleted = 0', (cutoff,))
        posts = self.cursor.fetchall()
        
        deleted = 0
        for post_id, msg_id, channel_id in posts:
            if msg_id and self.delete_message(channel_id, msg_id):
                self.cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?', (datetime.now().isoformat(), post_id))
                deleted += 1
        
        self.conn.commit()
        self.send_message(chat_id, f"✅ Deleted {deleted} posts older than {days} days")
        self.show_delete_posts_menu(chat_id)
    
    def show_delete_all_confirm(self, chat_id: int):
        """Confirm delete all posts"""
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        total = self.cursor.fetchone()[0]
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "⚠️ YES, DELETE EVERYTHING", "callback_data": "confirm_delete_all"}],
                [{"text": "❌ Cancel", "callback_data": "delete_posts_menu"}]
            ]
        }
        
        self.send_message(chat_id, f"🚨 <b>DELETE ALL {total} POSTS?</b>\n\nThis PERMANENTLY removes all messages from all channels!\n\nType CONFIRM to proceed:", keyboard)
        self.pending_requests[chat_id] = {'action': 'confirm_delete_all'}
    
    def execute_delete_all(self, chat_id: int):
        """Delete all posts"""
        self.cursor.execute('SELECT id, message_id, channel_id FROM posted_messages WHERE is_deleted = 0')
        posts = self.cursor.fetchall()
        
        deleted = 0
        for post_id, msg_id, channel_id in posts:
            if msg_id:
                self.unpin_message(channel_id, msg_id)
                if self.delete_message(channel_id, msg_id):
                    self.cursor.execute('UPDATE posted_messages SET is_deleted = 1, deleted_at = ? WHERE id = ?', (datetime.now().isoformat(), post_id))
                    deleted += 1
        
        self.conn.commit()
        self.send_message(chat_id, f"✅ Deleted ALL {deleted} posts!")
        self.show_main_menu(chat_id)
    
    # ============ FORWARD HANDLING ============
    
    def extract_message_data(self, message: Dict) -> Dict:
        """Extract message data"""
        data = {
            'source_chat_id': message.get('chat', {}).get('id'),
            'source_message_id': message.get('message_id'),
            'text': message.get('text', ''),
            'caption': message.get('caption', ''),
            'media_type': None,
            'media_file_id': None,
            'has_premium_emoji': False
        }
        
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
    
    def handle_forwarded_message(self, message: Dict, chat_id: int, user_id: int):
        """Handle forwarded message"""
        if user_id != self.admin_id:
            self.send_message(chat_id, "⛔ Unauthorized!")
            return
        
        msg_data = self.extract_message_data(message)
        
        today = datetime.now().date().isoformat()
        self.cursor.execute('INSERT INTO stats (date, ads_received) VALUES (?, 1) ON CONFLICT(date) DO UPDATE SET ads_received = ads_received + 1', (today,))
        self.conn.commit()
        
        self.pending_requests[chat_id] = msg_data
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "📤 Post Now", "callback_data": "forward_now"}],
                [{"text": "📌 Post & Pin (1 Hour)", "callback_data": "forward_pin_3600"}],
                [{"text": "📌 Post & Pin (1 Day)", "callback_data": "forward_pin_86400"}],
                [{"text": "🗑️ Post & Auto-Delete (1 Day)", "callback_data": "forward_delete_86400"}],
                [{"text": "📌 Pin & Delete (1 Hour Pin, 1 Day Delete)", "callback_data": "forward_pin_delete"}],
                [{"text": "⏲️ Create Auto-Timer", "callback_data": "forward_timer"}],
                [{"text": "❌ Cancel", "callback_data": "cancel"}]
            ]
        }
        
        preview = msg_data['text'][:100] if msg_data['text'] else f"📷 {msg_data['media_type'] or 'Media'}"
        self.send_message(chat_id, f"✅ Message received!\n\nPreview: {preview}\n\nChoose option:", keyboard)
    
    def execute_forward(self, chat_id: int, pin: bool = False, pin_duration: int = 0, delete_after: int = 0):
        """Execute forward with options"""
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "❌ No message found")
            return
        
        pending = self.pending_requests[chat_id]
        channels = self.channel_manager.get_all_channels(only_active=True)
        
        if not channels:
            self.send_message(chat_id, "❌ No active channels")
            return
        
        success_count = 0
        for ch in channels:
            success, msg_id = self.forward_message_to_channel_with_response(
                ch['channel_id'], 
                str(pending['source_chat_id']), 
                pending['source_message_id']
            )
            
            if success and msg_id:
                success_count += 1
                self.channel_manager.update_post_count(ch['channel_id'])
                
                if pin and pin_duration > 0:
                    self.pin_message(ch['channel_id'], msg_id, pin_duration)
                
                if delete_after > 0:
                    self.schedule_message_deletion(ch['channel_id'], msg_id, 0, delete_after)
                
                self.record_posted_message(msg_id, ch['channel_id'], 0, pin, pin_duration, delete_after)
        
        today = datetime.now().date().isoformat()
        self.cursor.execute('UPDATE stats SET ads_posted = ads_posted + ? WHERE date = ?', (success_count, today))
        self.conn.commit()
        
        msg = f"✅ Posted to {success_count}/{len(channels)} channels!"
        if pin: msg += f"\n📌 Pinned for {pin_duration//3600}h"
        if delete_after: msg += f"\n🗑️ Will delete in {delete_after//3600}h"
        
        self.send_message(chat_id, msg)
        self.pending_requests.pop(chat_id, None)
    
    def create_timer(self, chat_id: int, interval_value: int, interval_unit: str, pin: bool = False, pin_duration: int = 0, delete_after: int = 0):
        """Create a timer"""
        if chat_id not in self.pending_requests:
            self.send_message(chat_id, "❌ No message found")
            return
        
        pending = self.pending_requests[chat_id]
        channels = self.channel_manager.get_all_channels(only_active=True)
        channel_ids = [ch['channel_id'] for ch in channels]
        channels_json = json.dumps(channel_ids)
        
        self.cursor.execute('''
            INSERT INTO ads (content, ad_type, status, created_at, source_message_id, source_chat_id, target_channels, media_type, media_file_id, caption, pin, pin_duration, delete_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (pending.get('text', ''), 'forward', AdStatus.ACTIVE.value, datetime.now().isoformat(),
              pending['source_message_id'], str(pending['source_chat_id']), channels_json,
              pending.get('media_type'), pending.get('media_file_id'), pending.get('caption', ''),
              1 if pin else 0, pin_duration, delete_after))
        
        ad_id = self.cursor.lastrowid
        
        now = datetime.now()
        if interval_unit == 'minutes':
            first_run = now + timedelta(minutes=interval_value)
        elif interval_unit == 'hours':
            first_run = now + timedelta(hours=interval_value)
        else:
            first_run = now + timedelta(days=interval_value)
        
        self.cursor.execute('''
            INSERT INTO timers (ad_id, interval_value, interval_unit, next_run, created_at, channel_ids, created_from_time, timer_type, pin, pin_duration, delete_after)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (ad_id, interval_value, interval_unit, first_run.isoformat(), now.isoformat(), channels_json, now.isoformat(), 'forward',
              1 if pin else 0, pin_duration, delete_after))
        
        timer_id = self.cursor.lastrowid
        self.cursor.execute('UPDATE ads SET timer_id = ? WHERE id = ?', (timer_id, ad_id))
        self.conn.commit()
        
        pin_text = f"📌 Auto-pin: {pin_duration//3600}h\n" if pin else ""
        delete_text = f"🗑️ Auto-delete: {delete_after//3600}h\n" if delete_after > 0 else ""
        
        self.send_message(chat_id, f"✅ Timer #{timer_id} created!\n⏰ Every {interval_value} {interval_unit}\n{pin_text}{delete_text}🎯 First run: {first_run.strftime('%H:%M:%S')}")
        self.pending_requests.pop(chat_id, None)
    
    # ============ CHANNEL MANAGEMENT ============
    
    def show_channel_menu(self, chat_id: int):
        """Show channel menu"""
        keyboard = {
            "inline_keyboard": [
                [{"text": "➕ Add Channel", "callback_data": "ch_add"}],
                [{"text": "📋 List Channels", "callback_data": "ch_list"}],
                [{"text": "⭐ Set Default", "callback_data": "ch_default"}],
                [{"text": "🔘 Toggle Channel", "callback_data": "ch_toggle"}],
                [{"text": "❌ Remove Channel", "callback_data": "ch_remove"}],
                [{"text": "◀️ Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        self.send_message(chat_id, "🔧 Channel Management", keyboard)
    
    def add_channel_handler(self, chat_id: int):
        self.send_message(chat_id, "Send channel ID: @username or -1001234567890")
        self.pending_requests[chat_id] = {'action': 'add_channel'}
    
    def list_channels_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=False)
        if not channels:
            self.send_message(chat_id, "No channels")
            return
        
        text = "📋 Channels:\n\n"
        for ch in channels:
            text += f"{'⭐' if ch['is_default'] else '📢'} {ch['name']}\n"
            text += f"ID: {ch['channel_id']}\n"
            text += f"Status: {'✅' if ch['is_active'] else '❌'}\n"
            text += f"Posts: {ch['post_count']}\n\n"
        
        self.send_message(chat_id, text)
    
    def set_default_channel_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=True)
        if not channels:
            self.send_message(chat_id, "No active channels")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": ch['name'], "callback_data": f"set_default_{ch['channel_id']}"}]
                for ch in channels
            ]
        }
        self.send_message(chat_id, "Select default channel:", keyboard)
    
    def toggle_channel_handler(self, chat_id: int):
        channels = self.channel_manager.get_all_channels(only_active=False)
        keyboard = {
            "inline_keyboard": [
                [{"text": f"{'✅' if ch['is_active'] else '❌'} {ch['name']}", "callback_data": f"toggle_{ch['channel_id']}"}]
                for ch in channels
            ]
        }
        self.send_message(chat_id, "Toggle channel:", keyboard)
    
    def remove_channel_handler(self, chat_id: int):
        channels = [ch for ch in self.channel_manager.get_all_channels(only_active=False) if not ch['is_default']]
        if not channels:
            self.send_message(chat_id, "No removable channels")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"❌ {ch['name']}", "callback_data": f"remove_{ch['channel_id']}"}]
                for ch in channels
            ]
        }
        self.send_message(chat_id, "Select channel to remove:", keyboard)
    
    # ============ TIMER MANAGEMENT ============
    
    def show_timer_menu(self, chat_id: int):
        """Show timer menu"""
        self.cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active = self.cursor.fetchone()[0]
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "⏲️ Create Timer", "callback_data": "timer_create"}],
                [{"text": "📋 List Timers", "callback_data": "timer_list"}],
                [{"text": "⏸️ Pause Timer", "callback_data": "timer_pause"}],
                [{"text": "❌ Delete Timer", "callback_data": "timer_delete"}],
                [{"text": "◀️ Back to Main", "callback_data": "back_to_main"}]
            ]
        }
        self.send_message(chat_id, f"⏲️ Timer Management\nActive: {active}", keyboard)
    
    def show_timer_interval_menu(self, chat_id: int):
        keyboard = {
            "inline_keyboard": [
                [{"text": "1 minute", "callback_data": "timer_int_1_minutes"}],
                [{"text": "5 minutes", "callback_data": "timer_int_5_minutes"}],
                [{"text": "10 minutes", "callback_data": "timer_int_10_minutes"}],
                [{"text": "15 minutes", "callback_data": "timer_int_15_minutes"}],
                [{"text": "30 minutes", "callback_data": "timer_int_30_minutes"}],
                [{"text": "1 hour", "callback_data": "timer_int_1_hours"}],
                [{"text": "2 hours", "callback_data": "timer_int_2_hours"}],
                [{"text": "4 hours", "callback_data": "timer_int_4_hours"}],
                [{"text": "6 hours", "callback_data": "timer_int_6_hours"}],
                [{"text": "12 hours", "callback_data": "timer_int_12_hours"}],
                [{"text": "1 day", "callback_data": "timer_int_1_days"}],
                [{"text": "◀️ Back", "callback_data": "timer_menu"}]
            ]
        }
        self.send_message(chat_id, "Select interval:", keyboard)
    
    def list_timers_handler(self, chat_id: int):
        self.cursor.execute('SELECT id, interval_value, interval_unit, next_run, total_runs, max_runs FROM timers WHERE is_active = 1')
        timers = self.cursor.fetchall()
        
        if not timers:
            self.send_message(chat_id, "No active timers")
            return
        
        text = "⏲️ Active Timers:\n\n"
        for t in timers:
            timer_id, val, unit, next_run, runs, max_runs = t
            next_time = datetime.fromisoformat(next_run)
            remaining = next_time - datetime.now()
            minutes = remaining.seconds // 60
            text += f"#{timer_id}: Every {val} {unit}\nNext: {minutes}m\nRuns: {runs}/{max_runs or '∞'}\n\n"
        
        self.send_message(chat_id, text)
    
    def pause_timer_selection(self, chat_id: int):
        self.cursor.execute('SELECT id, interval_value, interval_unit FROM timers WHERE is_active = 1')
        timers = self.cursor.fetchall()
        
        if not timers:
            self.send_message(chat_id, "No active timers")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"Timer #{t[0]} - Every {t[1]} {t[2]}", "callback_data": f"pause_timer_{t[0]}"}]
                for t in timers
            ]
        }
        self.send_message(chat_id, "Select timer to pause:", keyboard)
    
    def delete_timer_selection(self, chat_id: int):
        self.cursor.execute('SELECT id, interval_value, interval_unit FROM timers')
        timers = self.cursor.fetchall()
        
        if not timers:
            self.send_message(chat_id, "No timers")
            return
        
        keyboard = {
            "inline_keyboard": [
                [{"text": f"❌ Timer #{t[0]} - Every {t[1]} {t[2]}", "callback_data": f"delete_timer_{t[0]}"}]
                for t in timers
            ]
        }
        self.send_message(chat_id, "Select timer to delete:", keyboard)
    
    # ============ MAIN MENU ============
    
    def show_main_menu(self, chat_id: int):
        """Show main menu"""
        default_ch = self.channel_manager.get_default_channel() or 'Not set'
        self.cursor.execute('SELECT COUNT(*) FROM timers WHERE is_active = 1')
        active_timers = self.cursor.fetchone()[0]
        
        keyboard = {
            "inline_keyboard": [
                [{"text": "📤 Post Message", "callback_data": "forward_prompt"}],
                [{"text": "⏲️ Auto-Timers", "callback_data": "timer_menu"}],
                [{"text": "🔧 Manage Channels", "callback_data": "channel_menu"}],
                [{"text": "🗑️ Delete Posts", "callback_data": "delete_posts_menu"}],
                [{"text": "📊 Statistics", "callback_data": "stats_main"}],
                [{"text": "❓ Help", "callback_data": "help_main"}]
            ]
        }
        
        text = f"""
🤖 <b>Ad Bot with Auto-Pin & Auto-Delete</b>

📡 Default: {default_ch}
⏲️ Active Timers: {active_timers}

<b>Features:</b>
• Forward messages to channels
• Auto-pin with custom duration
• Auto-delete old posts
• Repeating timers
• Bulk delete options
        """
        self.send_message(chat_id, text, keyboard)
    
    def show_stats(self, chat_id: int):
        """Show statistics"""
        today = datetime.now().date().isoformat()
        self.cursor.execute('SELECT ads_posted, ads_received, messages_deleted FROM stats WHERE date = ?', (today,))
        today_stats = self.cursor.fetchone()
        
        channels = self.channel_manager.get_all_channels(only_active=False)
        self.cursor.execute('SELECT COUNT(*) FROM posted_messages WHERE is_deleted = 0')
        active_posts = self.cursor.fetchone()[0]
        
        text = f"""
📊 <b>Statistics</b>

<b>Today:</b>
📥 Received: {today_stats[1] if today_stats else 0}
📤 Posted: {today_stats[0] if today_stats else 0}
🗑️ Deleted: {today_stats[2] if today_stats else 0}

<b>Overall:</b>
📢 Channels: {len(channels)}
📝 Active Posts: {active_posts}
        """
        self.send_message(chat_id, text)
    
    def show_help(self, chat_id: int):
        """Show help"""
        text = """
📚 <b>Help Guide</b>

<b>Quick Start:</b>
1. Add channel: /channels
2. Forward any message to me
3. Choose post option

<b>Post Options:</b>
• Post Now - Direct forward
• Post & Pin - Pin for 1h/1d
• Post & Delete - Auto-delete
• Pin & Delete - Both

<b>Timer Options:</b>
• Create repeating posts
• Can pin/delete automatically

<b>Delete Options:</b>
• Single post by ID
• By channel
• All pinned posts
• Expired posts
• Delete everything
        """
        self.send_message(chat_id, text)
    
    # ============ MESSAGE HANDLERS ============
    
    def send_message(self, chat_id: int, text: str, keyboard: Dict = None):
        """Send message"""
        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        if keyboard:
            payload["reply_markup"] = keyboard
        
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            logging.error(f"Send error: {e}")
    
    def answer_callback(self, callback_id: str, text: str = None):
        """Answer callback"""
        url = f"{self.base_url}/answerCallbackQuery"
        payload = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        try:
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            pass
    
    def handle_text_input(self, chat_id: int, text: str, user_id: int):
        """Handle text input"""
        if user_id != self.admin_id:
            return
        
        if chat_id in self.pending_requests:
            req = self.pending_requests[chat_id]
            
            if req.get('action') == 'add_channel':
                parts = text.split(',', 1)
                channel_id = parts[0].strip()
                name = parts[1].strip() if len(parts) > 1 else channel_id
                self.channel_manager.add_channel(channel_id, name)
                self.send_message(chat_id, f"✅ Added {name}")
                self.pending_requests.pop(chat_id)
                self.show_channel_menu(chat_id)
                return
            
            elif req.get('action') == 'confirm_delete_all' and text == 'CONFIRM':
                self.execute_delete_all(chat_id)
                self.pending_requests.pop(chat_id)
                return
        
        if text.startswith('/'):
            cmd = text.lower()
            if cmd == '/start':
                self.show_main_menu(chat_id)
            elif cmd == '/channels':
                self.show_channel_menu(chat_id)
            elif cmd == '/stats':
                self.show_stats(chat_id)
            elif cmd == '/help':
                self.show_help(chat_id)
    
    # ============ CALLBACK HANDLER ============
    
    def handle_callback(self, callback_data: str, chat_id: int, message_id: int, user_id: int, callback_id: str = None):
        """Handle callback queries"""
        if user_id != self.admin_id:
            if callback_id:
                self.answer_callback(callback_id, "Unauthorized!")
            return
        
        # Forward options
        if callback_data == "forward_prompt":
            self.send_message(chat_id, "Forward a message to me!")
        
        elif callback_data == "forward_now":
            self.execute_forward(chat_id, pin=False, pin_duration=0, delete_after=0)
        
        elif callback_data == "forward_pin_3600":
            self.execute_forward(chat_id, pin=True, pin_duration=3600, delete_after=0)
        
        elif callback_data == "forward_pin_86400":
            self.execute_forward(chat_id, pin=True, pin_duration=86400, delete_after=0)
        
        elif callback_data == "forward_delete_86400":
            self.execute_forward(chat_id, pin=False, pin_duration=0, delete_after=86400)
        
        elif callback_data == "forward_pin_delete":
            self.execute_forward(chat_id, pin=True, pin_duration=3600, delete_after=86400)
        
        elif callback_data == "forward_timer":
            self.show_timer_interval_menu(chat_id)
            self.pending_requests[chat_id] = self.pending_requests.get(chat_id, {})
            self.pending_requests[chat_id]['awaiting_timer'] = True
        
        # Timer intervals
        elif callback_data.startswith("timer_int_"):
            parts = callback_data.replace("timer_int_", "").split("_")
            val = int(parts[0])
            unit = parts[1]
            if self.pending_requests.get(chat_id, {}).get('awaiting_timer'):
                self.create_timer(chat_id, val, unit, pin=False)
        
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
        elif callback_data.startswith("pause_timer_"):
            tid = int(callback_data.replace("pause_timer_", ""))
            self.cursor.execute('UPDATE timers SET is_active = 0 WHERE id = ?', (tid,))
            self.conn.commit()
            self.send_message(chat_id, f"✅ Timer #{tid} paused")
        elif callback_data.startswith("delete_timer_"):
            tid = int(callback_data.replace("delete_timer_", ""))
            self.cursor.execute('DELETE FROM timers WHERE id = ?', (tid,))
            self.conn.commit()
            self.send_message(chat_id, f"✅ Timer #{tid} deleted")
        
        # Channel management
        elif callback_data == "channel_menu":
            self.show_channel_menu(chat_id)
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
        elif callback_data.startswith("set_default_"):
            ch_id = callback_data.replace("set_default_", "")
            self.channel_manager.set_default_channel(ch_id)
            self.send_message(chat_id, f"✅ Default: {ch_id}")
        elif callback_data.startswith("toggle_"):
            ch_id = callback_data.replace("toggle_", "")
            ch_list = self.channel_manager.get_all_channels(only_active=False)
            for ch in ch_list:
                if ch['channel_id'] == ch_id:
                    self.channel_manager.toggle_channel(ch_id, not ch['is_active'])
                    self.send_message(chat_id, f"✅ Toggled {ch['name']}")
                    break
        elif callback_data.startswith("remove_"):
            ch_id = callback_data.replace("remove_", "")
            if self.channel_manager.remove_channel(ch_id):
                self.send_message(chat_id, f"✅ Removed channel")
        
        # Delete posts
        elif callback_data == "delete_posts_menu":
            self.show_delete_posts_menu(chat_id)
        elif callback_data == "delete_single_post":
            self.list_all_posts(chat_id)
        elif callback_data == "delete_by_channel":
            self.show_delete_by_channel(chat_id)
        elif callback_data == "delete_pinned_posts":
            self.show_delete_pinned_posts(chat_id)
        elif callback_data == "delete_expired_posts":
            self.show_delete_expired_posts(chat_id)
        elif callback_data == "delete_all_posts_confirm":
            self.show_delete_all_confirm(chat_id)
        elif callback_data == "list_all_posts":
            self.list_all_posts(chat_id)
        elif callback_data.startswith("list_posts_page_"):
            page = int(callback_data.replace("list_posts_page_", ""))
            self.list_all_posts(chat_id, page)
        elif callback_data.startswith("delete_channel_"):
            ch_id = callback_data.replace("delete_channel_", "")
            self.confirm_delete_channel(chat_id, ch_id)
        elif callback_data.startswith("confirm_del_channel_"):
            ch_id = callback_data.replace("confirm_del_channel_", "")
            self.execute_delete_channel(chat_id, ch_id)
        elif callback_data == "confirm_delete_pinned":
            self.execute_delete_pinned(chat_id)
        elif callback_data.startswith("delete_expired_"):
            days = int(callback_data.replace("delete_expired_", ""))
            self.execute_delete_expired(chat_id, days)
        elif callback_data == "confirm_delete_all":
            self.execute_delete_all(chat_id)
        
        # Stats and help
        elif callback_data == "stats_main":
            self.show_stats(chat_id)
        elif callback_data == "help_main":
            self.show_help(chat_id)
        elif callback_data == "back_to_main":
            self.show_main_menu(chat_id)
        elif callback_data == "cancel":
            self.pending_requests.pop(chat_id, None)
            self.send_message(chat_id, "Cancelled")
        
        if callback_id:
            self.answer_callback(callback_id)
    
    # ============ MAIN LOOP ============
    
    def run(self):
        """Main bot loop"""
        logging.info("🤖 BOT STARTED!")
        self.timer_manager.start_timer_processor()
        
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
                        
                        if 'forward_date' in msg or 'forward_from' in msg:
                            self.handle_forwarded_message(msg, chat_id, user_id)
                        elif 'text' in msg:
                            self.handle_text_input(chat_id, msg['text'], user_id)
                    
                    elif 'callback_query' in update:
                        cb = update['callback_query']
                        self.handle_callback(
                            cb['data'],
                            cb['message']['chat']['id'],
                            cb['message']['message_id'],
                            cb['from']['id'],
                            cb['id']
                        )
                
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Main loop error: {e}")
                time.sleep(5)
    
    def stop(self):
        self.running = False
        self.timer_manager.running = False
        self.conn.close()
        logging.info("Bot stopped")

# ============ RUN ============
if __name__ == "__main__":
    BOT_TOKEN = "8471906303:AAGhn4WDzPbe-uERvYtVZoFgD5dNk2e2sLY"
    ADMIN_ID = 7049142115
    
    bot = ForwardAdBot(BOT_TOKEN, ADMIN_ID)
    
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.stop()
        print("\n✅ Bot stopped")