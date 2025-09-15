import pandas as pd
import numpy as np
import aiohttp
import asyncio
import time
import warnings
from datetime import datetime, timezone, timedelta
import requests
from typing import List
from collections import deque
import threading
import pytz

warnings.filterwarnings('ignore')

# ================== PUSHBULLET ==================
PUSHBULLET_TOKEN = "o.SJ5wXkGzsBaU9W1kyMqLsIz8kEYJXP4Z"

# Pakistan timezone
PKT = pytz.timezone('Asia/Karachi')

def get_pakistan_time():
    """Get current time in Pakistan timezone"""
    return datetime.now(PKT)

def format_pakistan_time(dt=None):
    """Format Pakistan time for notifications"""
    if dt is None:
        dt = get_pakistan_time()
    elif dt.tzinfo is None:
        # Convert UTC to Pakistan time if timezone is not set
        dt = pytz.UTC.localize(dt).astimezone(PKT)
    elif dt.tzinfo != PKT:
        # Convert to Pakistan time if different timezone
        dt = dt.astimezone(PKT)
    
    return dt.strftime('%d-%m-%Y %H:%M:%S PKT')

class NotificationManager:
    def __init__(self):
        self.notification_queue = deque()
        self.last_sent_time = 0
        self.min_interval = 2  # Minimum 2 seconds between notifications
        self.max_retries = 3
        self.failed_notifications = []
        
        # Start background notification sender
        self.notification_thread = threading.Thread(target=self._notification_worker, daemon=True)
        self.notification_thread.start()
    
    def _notification_worker(self):
        """Background worker to send notifications with proper rate limiting"""
        while True:
            try:
                if self.notification_queue:
                    current_time = time.time()
                    
                    # Rate limiting check
                    if current_time - self.last_sent_time >= self.min_interval:
                        notification = self.notification_queue.popleft()
                        success = self._send_immediate(notification)
                        
                        if success:
                            self.last_sent_time = current_time
                            print(f"✅ Notification sent: {notification['title']}")
                        else:
                            # Add to failed queue for retry
                            notification['retry_count'] = notification.get('retry_count', 0) + 1
                            if notification['retry_count'] <= self.max_retries:
                                self.failed_notifications.append(notification)
                                print(f"⚠️ Notification failed, will retry: {notification['title']}")
                
                # Retry failed notifications
                if self.failed_notifications and time.time() - self.last_sent_time >= 5:
                    failed_notif = self.failed_notifications.pop(0)
                    success = self._send_immediate(failed_notif)
                    if success:
                        self.last_sent_time = time.time()
                        print(f"✅ Retry successful: {failed_notif['title']}")
                    elif failed_notif['retry_count'] < self.max_retries:
                        self.failed_notifications.append(failed_notif)
                
                time.sleep(0.5)  # Check every 500ms
                
            except Exception as e:
                print(f"❌ Notification worker error: {e}")
                time.sleep(1)
    
    def _send_immediate(self, notification):
        """Send notification immediately"""
        try:
            data_send = {
                "type": "note", 
                "title": notification['title'], 
                "body": notification['body']
            }
            response = requests.post(
                "https://api.pushbullet.com/v2/pushes",
                json=data_send,
                headers={
                    "Access-Token": PUSHBULLET_TOKEN, 
                    "Content-Type": "application/json"
                },
                timeout=5
            )
            
            if response.status_code == 200:
                return True
            else:
                print(f"⚠️ Pushbullet Error: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Pushbullet Exception: {e}")
            return False
    
    def add_notification(self, title, body):
        """Add notification to queue"""
        current_pkt_time = get_pakistan_time()
        notification = {
            'title': title,
            'body': body,
            'timestamp': current_pkt_time
        }
        
        # Check for duplicate recent notifications
        for notif in list(self.notification_queue):
            if (notif['title'] == title and 
                (current_pkt_time - notif['timestamp']).total_seconds() < 60):
                print(f"🔄 Duplicate notification skipped: {title}")
                return
        
        self.notification_queue.append(notification)
        print(f"📝 Notification queued: {title} | {format_pakistan_time(current_pkt_time)}")

# Global notification manager
notif_manager = NotificationManager()

def send_notification(title, body):
    """Enhanced notification function with queue management"""
    notif_manager.add_notification(title, body)

# ================== RSI CLASS ==================
class RSIIndicator:
    def __init__(self, length=14):
        self.length = length

    def rma(self, series, length):
        alpha = 1.0 / length
        return series.ewm(alpha=alpha, adjust=False).mean()

    def calculate_rsi(self, close_prices):
        if len(close_prices) < self.length + 1:
            return pd.Series([np.nan] * len(close_prices), index=close_prices.index)

        changes = close_prices.diff()
        gains = changes.where(changes > 0, 0.0)
        losses = (-changes).where(changes < 0, 0.0)

        avg_gains = self.rma(gains, self.length)
        avg_losses = self.rma(losses, self.length)

        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))

        return rsi.fillna(50)

# ================== SUPPORT/RESISTANCE ==================
class SupportResistanceDetector:
    def __init__(self, lookback_period=20):
        self.lookback_period = lookback_period

    def find_support_resistance(self, df):
        if len(df) < self.lookback_period:
            return None

        recent_data = df.tail(self.lookback_period)

        support_idx = recent_data['low'].idxmin()
        support_level = recent_data.loc[support_idx, 'low']

        resistance_idx = recent_data['high'].idxmax()
        resistance_level = recent_data.loc[resistance_idx, 'high']

        return {
            'support_level': support_level,
            'resistance_level': resistance_level
        }

# ================== GET TOP PERPETUALS ==================
def get_top_n_perpetuals(n: int = 100, quote_filter: str = "USDT") -> List[str]:
    """
    Binance Futures 24h ticker سے top N symbols واپس کریں (highest quoteVolume کی بنیاد پر)
    """
    try:
        url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        filtered = [
            item for item in data
            if item.get('symbol', '').endswith(quote_filter)
            and ('DOWN' not in item['symbol'] and 'UP' not in item['symbol'])
        ]

        for item in filtered:
            try:
                item['quoteVolume'] = float(item.get('quoteVolume', 0) or 0)
            except:
                item['quoteVolume'] = 0.0

        filtered_sorted = sorted(filtered, key=lambda x: x['quoteVolume'], reverse=True)
        top_symbols = [item['symbol'] for item in filtered_sorted[:n]]

        print(f"✅ Fetched {len(top_symbols)} top perpetual symbols from Binance")
        return top_symbols

    except Exception as e:
        print(f"⚠️ Error fetching top perpetuals: {e}")
        return []

# ================== SCANNER ==================
class UltraFastRSIScanner:
    def __init__(self, symbols: List[str] = None):
        self.base_url = "https://fapi.binance.com/fapi/v1/klines"

        if symbols is None:
            symbols = get_top_n_perpetuals(100)  # Top 100 coins by volume
            if not symbols:
                symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT']
                print("⚠️ Using fallback symbols")

        self.symbols = symbols

        self.rsi_1m = RSIIndicator(length=14)
        self.rsi_15m = RSIIndicator(length=14)
        self.sr_detector = SupportResistanceDetector(lookback_period=20)

        self.data_1m = {}
        self.data_15m = {}
        self.last_signals = {symbol: {'type': None, 'time': None, 'candle_index': None} for symbol in self.symbols}
        self.active_signals = []
        
        # Signal cooldown to prevent spam
        self.signal_cooldown = {}

        for symbol in self.symbols:
            self.data_1m[symbol] = pd.DataFrame()
            self.data_15m[symbol] = pd.DataFrame()

    async def fetch_klines_direct(self, session, symbol, interval, limit=100):
        try:
            url = f"{self.base_url}?symbol={symbol}&interval={interval}&limit={limit}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as response:
                if response.status == 200:
                    data = await response.json()
                    df = pd.DataFrame(data, columns=[
                        'timestamp', 'open', 'high', 'low', 'close', 'volume',
                        'close_time', 'quote_volume', 'count', 'taker_buy_base',
                        'taker_buy_quote', 'ignore'
                    ])
                    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
                    df = df.astype({
                        'open': float, 'high': float, 'low': float,
                        'close': float, 'volume': float
                    })
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                    df.set_index('timestamp', inplace=True)
                    return df
                else:
                    return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    async def initialize_data(self):
        print("📊 Loading initial data...")
        connector = aiohttp.TCPConnector(limit=50)  # Increased for 100 coins
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for symbol in self.symbols:
                tasks.append(self.fetch_klines_direct(session, symbol, '1m', 100))
                tasks.append(self.fetch_klines_direct(session, symbol, '15m', 100))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, symbol in enumerate(self.symbols):
                result_1m = results[i * 2]
                if isinstance(result_1m, pd.DataFrame) and not result_1m.empty:
                    self.data_1m[symbol] = result_1m
                result_15m = results[i * 2 + 1]
                if isinstance(result_15m, pd.DataFrame) and not result_15m.empty:
                    self.data_15m[symbol] = result_15m

    async def update_all_data(self):
        connector = aiohttp.TCPConnector(limit=50)  # Increased for 100 coins
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = []
            for symbol in self.symbols:
                tasks.append(self.fetch_klines_direct(session, symbol, '1m', 2))
                tasks.append(self.fetch_klines_direct(session, symbol, '15m', 2))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, symbol in enumerate(self.symbols):
                try:
                    result_1m = results[i * 2]
                    if isinstance(result_1m, pd.DataFrame) and not result_1m.empty:
                        latest_1m = result_1m.iloc[-1:]
                        if len(self.data_1m[symbol]) > 0:
                            last_timestamp = self.data_1m[symbol].index[-1]
                            new_timestamp = latest_1m.index[0]
                            if new_timestamp == last_timestamp:
                                self.data_1m[symbol].iloc[-1] = latest_1m.iloc[0]
                            elif new_timestamp > last_timestamp:
                                self.data_1m[symbol] = pd.concat([self.data_1m[symbol], latest_1m])
                                if len(self.data_1m[symbol]) > 150:
                                    self.data_1m[symbol] = self.data_1m[symbol].tail(150)
                    
                    result_15m = results[i * 2 + 1]
                    if isinstance(result_15m, pd.DataFrame) and not result_15m.empty:
                        latest_15m = result_15m.iloc[-1:]
                        if len(self.data_15m[symbol]) > 0:
                            last_timestamp = self.data_15m[symbol].index[-1]
                            new_timestamp = latest_15m.index[0]
                            if new_timestamp == last_timestamp:
                                self.data_15m[symbol].iloc[-1] = latest_15m.iloc[0]
                            elif new_timestamp > last_timestamp:
                                self.data_15m[symbol] = pd.concat([self.data_15m[symbol], latest_15m])
                                if len(self.data_15m[symbol]) > 150:
                                    self.data_15m[symbol] = self.data_15m[symbol].tail(150)
                except:
                    continue

    def analyze_symbol(self, symbol):
        try:
            df_1m = self.data_1m.get(symbol)
            df_15m = self.data_15m.get(symbol)
            if df_1m is None or df_15m is None or len(df_1m) < 30 or len(df_15m) < 30:
                return None

            rsi_1m_values = self.rsi_1m.calculate_rsi(df_1m['close'])
            rsi_15m_values = self.rsi_15m.calculate_rsi(df_15m['close'])

            current_rsi_1m = rsi_1m_values.iloc[-1]
            current_rsi_15m = rsi_15m_values.iloc[-1]

            if pd.isna(current_rsi_1m) or pd.isna(current_rsi_15m):
                return None

            sr_levels = self.sr_detector.find_support_resistance(df_15m)
            if sr_levels is None:
                return None

            current_candle = df_15m.iloc[-1]
            current_price = current_candle['close']

            support_touch = (current_candle['low'] <= sr_levels['support_level'] and
                             current_price > sr_levels['support_level'])
            resistance_touch = (current_candle['high'] >= sr_levels['resistance_level'] and
                                current_price < sr_levels['resistance_level'])

            signals = []
            current_time = get_pakistan_time()  # Use Pakistan time
            
            # Check signal cooldown
            cooldown_key = f"{symbol}_signal"
            last_signal_time = self.signal_cooldown.get(cooldown_key, 0)
            if current_time.timestamp() - last_signal_time < 180:  # 3 minute cooldown
                return {
                    'symbol': symbol, 'price': current_price,
                    'rsi_1m': current_rsi_1m, 'rsi_15m': current_rsi_15m,
                    'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                    'support_touch': support_touch, 'resistance_touch': resistance_touch,
                    'signals': signals, 'last_update': df_1m.index[-1] if not df_1m.empty else None
                }

            # BUY Signal
            if (support_touch and current_rsi_1m <= 35 and current_rsi_15m <= 35):
                signal = {
                    'symbol': symbol, 'type': 'BUY', 'price': current_price,
                    'rsi_1m': current_rsi_1m, 'rsi_15m': current_rsi_15m,
                    'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                    'time': current_time
                }
                signals.append(signal)
                self.signal_cooldown[cooldown_key] = current_time.timestamp()
                
                # Enhanced notification with Pakistan time
                notification_body = (f"🎯 {symbol}\n"
                                   f"💰 Price: ${current_price:.4f}\n"
                                   f"📊 RSI: 1m={current_rsi_1m:.1f} | 15m={current_rsi_15m:.1f}\n"
                                   f"🎯 Support: ${sr_levels['support_level']:.4f}\n"
                                   f"🕐 Time: {format_pakistan_time(current_time)}")
                send_notification("🟢 BUY SIGNAL 🚀", notification_body)

            # SELL Signal
            elif (resistance_touch and current_rsi_1m >= 70 and current_rsi_15m >= 70):
                signal = {
                    'symbol': symbol, 'type': 'SELL', 'price': current_price,
                    'rsi_1m': current_rsi_1m, 'rsi_15m': current_rsi_15m,
                    'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                    'time': current_time
                }
                signals.append(signal)
                self.signal_cooldown[cooldown_key] = current_time.timestamp()
                
                # Enhanced notification with Pakistan time
                notification_body = (f"🎯 {symbol}\n"
                                   f"💰 Price: ${current_price:.4f}\n"
                                   f"📊 RSI: 1m={current_rsi_1m:.1f} | 15m={current_rsi_15m:.1f}\n"
                                   f"🎯 Resistance: ${sr_levels['resistance_level']:.4f}\n"
                                   f"🕐 Time: {format_pakistan_time(current_time)}")
                send_notification("🔴 SELL SIGNAL ❌", notification_body)

            return {
                'symbol': symbol, 'price': current_price,
                'rsi_1m': current_rsi_1m, 'rsi_15m': current_rsi_15m,
                'support_level': sr_levels['support_level'], 'resistance_level': sr_levels['resistance_level'],
                'support_touch': support_touch, 'resistance_touch': resistance_touch,
                'signals': signals, 'last_update': df_1m.index[-1] if not df_1m.empty else None
            }

        except Exception as e:
            return None

    def scan_all_symbols(self):
        results = []
        signals = []
        for symbol in self.symbols:
            result = self.analyze_symbol(symbol)
            if result:
                results.append(result)
                if result['signals']:
                    signals.extend(result['signals'])
        return results, signals

    def print_results(self, results, signals, fetch_time):
        current_time_pkt = format_pakistan_time()
        print("\033[2J\033[H", end="")
        print(f"🚀 ULTRA FAST RSI SCANNER: {current_time_pkt}")
        print(f"⚡ Fetch Time: {fetch_time:.2f}s | 💎 Coins: {len(results)}/100 | 📱 Queue: {len(notif_manager.notification_queue)} | 🔄 Failed: {len(notif_manager.failed_notifications)}")
        print("=" * 100)

        print("\n📌 ACTIVE SIGNALS (Last 3 min)")
        print("-" * 100)
        if self.active_signals:
            for sig in self.active_signals:
                sig_type = "🟢 BUY" if sig['type'] == 'BUY' else "🔴 SELL"
                sig_time_pkt = format_pakistan_time(sig['time'])
                print(f"{sig_type} - {sig['symbol']} | Price: ${sig['price']:.4f} | "
                      f"RSI 1m: {sig['rsi_1m']:.1f} | 15m: {sig['rsi_15m']:.1f} | "
                      f"🕐 {sig_time_pkt}")
        else:
            print("⚪ No active signals in last 3 minutes.")
        print("=" * 100)

        if signals:
            print("\n🚨 🚨 NEW SIGNALS 🚨 🚨")
            print("-" * 50)
            for signal in signals:
                signal_type = "🟢 BUY" if signal['type'] == 'BUY' else "🔴 SELL"
                print(f"{signal_type} - {signal['symbol']}")
                print(f"   💰 Price: ${signal['price']:.4f}")
                print(f"   📊 RSI 1m: {signal['rsi_1m']:.1f} | 15m: {signal['rsi_15m']:.1f}")
                print(f"   📈 S/R: ${signal['support_level']:.4f} / ${signal['resistance_level']:.4f}")
                print("-" * 50)

        print(f"\n📊 LIVE STATUS: (Monitoring {len(results)} symbols)")
        print("-" * 100)
        
        # Show only top signals and interesting pairs
        interesting_results = []
        for result in results:
            if (result['rsi_1m'] <= 35 or result['rsi_1m'] >= 70 or 
                result['rsi_15m'] <= 35 or result['rsi_15m'] >= 70 or
                result['support_touch'] or result['resistance_touch']):
                interesting_results.append(result)
        
        # If no interesting results, show top 15 by volume for better display
        if not interesting_results:
            interesting_results = sorted(results, key=lambda x: x['symbol'])[:15]
            
        print(f"{'COIN':<12} {'PRICE':<12} {'RSI-1m':<8} {'RSI-15m':<9} {'S/R':<5} {'STATUS':<12}")
        print("-" * 100)
        
        for result in interesting_results:
            sup_touch = "🟢" if result['support_touch'] else "⚪"
            res_touch = "🔴" if result['resistance_touch'] else "⚪"
            rsi_1m_color = "🔴" if result['rsi_1m'] >= 70 else "🟢" if result['rsi_1m'] <= 30 else "⚪"
            rsi_15m_color = "🔴" if result['rsi_15m'] >= 70 else "🟢" if result['rsi_15m'] <= 30 else "⚪"
            status = "⚡SIGNAL!" if result['signals'] else "👁️ WATCH"
            print(f"{result['symbol']:<12} ${result['price']:<11.4f} "
                  f"{result['rsi_1m']:<7.1f}{rsi_1m_color} {result['rsi_15m']:<8.1f}{rsi_15m_color} "
                  f"{sup_touch}{res_touch}   {status:<12}")
        
        print(f"\n💡 BUY: Support Touch + RSI≤35 | SELL: Resistance Touch + RSI≥70")
        print(f"📊 Notifications: Queue={len(notif_manager.notification_queue)}, Failed={len(notif_manager.failed_notifications)}")

# ================== MAIN LOOP ==================
async def main_loop():
    scanner = UltraFastRSIScanner()
    await scanner.initialize_data()

    while True:
        start_time = time.time()
        await scanner.update_all_data()
        results, signals = scanner.scan_all_symbols()
        
        # Update active signals
        current_time = get_pakistan_time()
        scanner.active_signals = [
            sig for sig in scanner.active_signals
            if (current_time - sig['time']).total_seconds() < 180
        ]
        if signals:
            scanner.active_signals.extend(signals)
        
        fetch_time = time.time() - start_time
        scanner.print_results(results, signals, fetch_time)
        
        # Adaptive sleep - faster when there are signals
        sleep_time = 2 if signals else 4
        await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    try:
        print("🚀 Starting Ultra Fast RSI Scanner with Enhanced Notifications...")
        print("📱 Notification system initialized with queue management")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("👋 Scanner stopped by user")
