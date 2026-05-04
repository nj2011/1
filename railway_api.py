#!/usr/bin/env python3
"""
Railway Database API Server for CODM Account Checker
Provides account database storage and comparison endpoints
"""

import os
import json
import hashlib
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
from pathlib import Path

app = Flask(__name__)
CORS(app)

# Database path - Railway provides persistent storage via volumes
DB_PATH = os.environ.get('DB_PATH', '/data/database.db')
ADMIN_SECRET = os.environ.get('ADMIN_SECRET', 'your-secret-key-change-this')

def init_database():
    """Initialize SQLite database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create accounts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT NOT NULL,
            password TEXT NOT NULL,
            uid TEXT,
            username TEXT,
            shell_balance INTEGER DEFAULT 0,
            email_display TEXT,
            formatted_mobile TEXT,
            country TEXT,
            nickname TEXT,
            fb_username TEXT,
            fb_link TEXT,
            fb_info TEXT,
            codm_level INTEGER DEFAULT 0,
            codm_region TEXT,
            codm_nickname TEXT,
            codm_uid TEXT,
            last_login_date TEXT,
            last_login_where TEXT,
            last_login_ip TEXT,
            last_login_country TEXT,
            bind_status TEXT,
            region_code TEXT,
            source TEXT,
            added_date TEXT,
            UNIQUE(account, password)
        )
    ''')
    
    # Create stats table for tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY,
            total_lines INTEGER DEFAULT 0,
            last_updated TEXT,
            added_by TEXT
        )
    ''')
    
    # Initialize stats if empty
    cursor.execute('SELECT COUNT(*) FROM stats')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
            INSERT INTO stats (total_lines, last_updated, added_by)
            VALUES (0, ?, 'system')
        ''', (datetime.now().isoformat(),))
    
    conn.commit()
    conn.close()
    print(f"[✓] Database initialized at {DB_PATH}")

def get_account_hash(account, password):
    """Generate unique hash for account-password pair"""
    return hashlib.sha256(f"{account}:{password}".encode()).hexdigest()

@app.route('/', methods=['GET'])
def home():
    """API status endpoint"""
    return jsonify({
        'status': 'online',
        'service': 'CODM Account Database API',
        'version': '1.0.0',
        'endpoints': [
            {'path': '/api/compare', 'method': 'POST', 'description': 'Compare combos with database'},
            {'path': '/api/add', 'method': 'POST', 'description': 'Add accounts to database'},
            {'path': '/api/stats', 'method': 'GET', 'description': 'Get database statistics'},
            {'path': '/api/health', 'method': 'GET', 'description': 'Health check'}
        ]
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint for Railway"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get total accounts
        cursor.execute('SELECT COUNT(*) FROM accounts')
        total_lines = cursor.fetchone()[0]
        
        # Get latest addition
        cursor.execute('''
            SELECT added_date, source FROM accounts 
            ORDER BY added_date DESC LIMIT 1
        ''')
        latest = cursor.fetchone()
        
        # Get stats from stats table
        cursor.execute('SELECT last_updated, added_by FROM stats LIMIT 1')
        stats = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_lines': total_lines,
            'latest_added': 0,  # Not tracking incrementally for now
            'date_added': latest[0] if latest else 'N/A',
            'added_by': stats[1] if stats else 'Unknown',
            'last_updated': stats[0] if stats else datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/compare', methods=['POST'])
def compare_combos():
    """
    Compare combos with database and return non-matching ones
    Expects POST data with 'combos' field containing newline-separated combos
    """
    try:
        data = request.get_json()
        if not data or 'combos' not in data:
            return jsonify({'success': False, 'error': 'Missing combos field'}), 400
        
        combos_text = data['combos']
        lines = [line.strip() for line in combos_text.split('\n') if line.strip() and ':' in line]
        
        if not lines:
            return jsonify({'success': False, 'error': 'No valid combos found'}), 400
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all existing combos from database
        cursor.execute('SELECT account, password FROM accounts')
        existing = {(row[0], row[1]) for row in cursor.fetchall()}
        
        matches = []
        non_matches = []
        
        for line in lines:
            if ':' in line:
                account, password = line.split(':', 1)
                account = account.strip()
                password = password.strip()
                
                if (account, password) in existing:
                    matches.append(line)
                else:
                    non_matches.append(line)
        
        conn.close()
        
        return jsonify({
            'success': True,
            'total_checked': len(lines),
            'matches': len(matches),
            'non_matches': len(non_matches),
            'non_matched_combos': non_matches
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/add', methods=['POST'])
def add_accounts():
    """
    Add accounts to database
    Expected format: {'accounts': [{'account': 'xxx', 'password': 'yyy', ...}]}
    """
    try:
        # Verify admin secret
        auth_header = request.headers.get('X-Admin-Secret')
        if auth_header != ADMIN_SECRET:
            return jsonify({'success': False, 'error': 'Unauthorized'}), 401
        
        data = request.get_json()
        if not data or 'accounts' not in data:
            return jsonify({'success': False, 'error': 'Missing accounts field'}), 400
        
        accounts = data['accounts']
        added_count = 0
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        for acc in accounts:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO accounts (
                        account, password, uid, username, shell_balance,
                        email_display, formatted_mobile, country, nickname,
                        fb_username, fb_link, fb_info, codm_level, codm_region,
                        codm_nickname, codm_uid, last_login_date, last_login_where,
                        last_login_ip, last_login_country, bind_status, region_code,
                        source, added_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    acc.get('account'), acc.get('password'), acc.get('uid', 'N/A'),
                    acc.get('username', 'N/A'), acc.get('shell_balance', 0),
                    acc.get('email_display', 'N/A'), acc.get('formatted_mobile', 'N/A'),
                    acc.get('country', 'N/A'), acc.get('nickname', 'N/A'),
                    acc.get('fb_username', 'N/A'), acc.get('fb_link', 'N/A'),
                    acc.get('fb_info', 'N/A'), acc.get('codm_level', 0),
                    acc.get('codm_region', 'N/A'), acc.get('codm_nickname', 'N/A'),
                    acc.get('codm_uid', 'N/A'), acc.get('last_login_date', 'N/A'),
                    acc.get('last_login_where', 'N/A'), acc.get('last_login_ip', 'N/A'),
                    acc.get('last_login_country', 'N/A'), acc.get('bind_status', 'N/A'),
                    acc.get('region_code', 'N/A'), acc.get('source', 'api'),
                    datetime.now().isoformat()
                ))
                if cursor.rowcount > 0:
                    added_count += 1
            except Exception as e:
                print(f"Error adding account: {e}")
                continue
        
        # Update stats
        cursor.execute('SELECT COUNT(*) FROM accounts')
        total = cursor.fetchone()[0]
        cursor.execute('''
            UPDATE stats SET total_lines = ?, last_updated = ? WHERE id = 1
        ''', (total, datetime.now().isoformat()))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'added': added_count,
            'total': total
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/search', methods=['POST'])
def search_account():
    """Search for a specific account in database"""
    try:
        data = request.get_json()
        account = data.get('account', '')
        password = data.get('password', '')
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if password:
            cursor.execute('SELECT * FROM accounts WHERE account = ? AND password = ?', (account, password))
        else:
            cursor.execute('SELECT * FROM accounts WHERE account = ?', (account,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return jsonify({'success': True, 'exists': True, 'data': dict(zip([d[0] for d in cursor.description], row))})
        else:
            return jsonify({'success': True, 'exists': False})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    # Create data directory if it doesn't exist
    Path('/data').mkdir(exist_ok=True)
    
    # Initialize database
    init_database()
    
    # Get port from environment (Railway sets this)
    port = int(os.environ.get('PORT', 8080))
    
    print(f"[✓] Starting API server on port {port}")
    app.run(host='0.0.0.0', port=port)