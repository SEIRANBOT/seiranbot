import os
import asyncio
import logging
import time
from aiohttp import ClientSession, ClientTimeout, WSMsgType, web
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('seiranbot')

TOKEN = os.getenv('BOT_TOKEN')
BASE_URL = 'https://data-api.binance.vision'
STREAM_URL = 'wss://data-stream.binance.vision/stream'
SYMBOLS = [
    'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT','ADAUSDT','DOGEUSDT','TRXUSDT',
    'LINKUSDT','AVAXUSDT','LTCUSDT','DOTUSDT','BCHUSDT','ETCUSDT','NEARUSDT','ATOMUSDT',
    'UNIUSDT','APTUSDT','FILUSDT','SUIUSDT'
]
TIMEFRAMES = ['1m','3m','5m','15m','30m','1h','2h','4h','6h','8h','12h','1d','3d','1w','1M']

http: ClientSession | None = None
ws_task: asyncio.Task | None = None
web_runner: web.AppRunner | None = None
price_cache: dict[str, float] = {}
candle_cache: dict[tuple[str, str], tuple[float, list]] = {}
known_symbols: set[str] = set(SYMBOLS)
symbols_loaded_at = 0.0
rest_blocked_until = 0.0


async def api_get(path: str, params: dict | None = None):
    global rest_blocked_until
    if time.monotonic() < rest_blocked_until:
        raise RuntimeError('Binance REST временно ограничил запросы. Подождите несколько минут.')
    if http is None:
        raise RuntimeError('HTTP-сессия ещё не запущена.')
    async with http.get(BASE_URL + path, params=params) as response:
        if response.status in (418, 429):
            rest_blocked_until = time.monotonic() + 300
            raise RuntimeError('Binance ограничил частоту запросов. Подождите 5 минут.')
        response.raise_for_status()
        return await response.json()


async def refresh_symbols():
    global known_symbols, symbols_loaded_at
    data = await api_get('/api/v3/exchangeInfo')
    known_symbols = {
        item['symbol'] for item in data.get('symbols', [])
        if item.get('quoteAsset') == 'USDT'
        and item.get('status') == 'TRADING'
        and item.get('isSpotTradingAllowed', True)
    }
    symbols_loaded_at = time.monotonic()


async def check_symbol(raw: str):
    symbol = raw.upper().replace('/', '').strip()
    if not symbol.endswith('USDT'):
        return None
    if time.monotonic() - symbols_loaded_at > 3600:
        try:
            await refresh_symbols()
        except Exception as exc:
            log.warning('Не обновлён список пар: %s', exc)
    return symbol if symbol in known_symbols else None


async def websocket_worker():
    while True:
        try:
            streams = '/'.join(f'{s.lower()}@kline_1m' for s in SYMBOLS)
            async with http.ws_connect(f'{STREAM_URL}?streams={streams}', heartbeat=30) as ws:
                log.info('Binance WebSocket подключён')
                async for msg in ws:
                    if msg.type == WSMsgType.TEXT:
                        payload = msg.json().get('data', {})
                        kline = payload.get('k')
                        if kline:
                            price_cache[kline['s']] = float(kline['c'])
                    elif msg.type in (WSMsgType.ERROR, WSMsgType.CLOSED):
                        break
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning('WebSocket ошибка: %s', exc)
        await asyncio.sleep(10)


async def get_price(symbol: str) -> float:
    if symbol in price_cache:
        return price_cache[symbol]
    data = await api_get('/api/v3/ticker/price', {'symbol': symbol})
    return float(data['price'])


async def get_candles(symbol: str, interval: str = '1m', limit: int = 200):
    if interval not in TIMEFRAMES:
        raise ValueError('Неверный таймфрейм. Используйте /timeframes.')
    key = (symbol, interval)
    now = time.monotonic()
    cached = candle_cache.get(key)
    if cached and now - cached[0] < 20:
        return cached[1]
    rows = await api_get('/api/v3/klines', {'symbol': symbol, 'interval': interval, 'limit': limit})
    candles = [{'open': float(x[1]), 'high': float(x[2]), 'low': float(x[3]),
                'close': float(x[4]), 'volume': float(x[5])} for x in rows]
    candle_cache[key] = (now, candles)
    return candles


def sma(values, period):
    return sum(values[-period:]) / period if len(values) >= period else None


def ema(values, period):
    if len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    for item in values[period:]:
        value = alpha * item + (1 - alpha) * value
    return value


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0) for x in changes[-period:]]
    losses = [max(-x, 0) for x in changes[-period:]]
    avg_gain, avg_loss = sum(gains) / period, sum(losses) / period
    if avg_loss == 0:
        return 100.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def macd(values):
    fast, slow = ema(values, 12), ema(values, 26)
    return fast - slow if fast is not None and slow is not None else None


def bollinger(values, period=20):
    middle = sma(values, period)
    if middle is None:
        return None
    variance = sum((x - middle) ** 2 for x in values[-period:]) / period
    deviation = variance ** 0.5
    return middle + 2 * deviation, middle, middle - 2 * deviation


def stochastic(candles, period=14):
    if len(candles) < period:
        return None
    recent = candles[-period:]
    high = max(x['high'] for x in recent)
    low = min(x['low'] for x in recent)
    return 50.0 if high == low else 100 * (candles[-1]['close'] - low) / (high - low)


def cci(candles, period=20):
    if len(candles) < period:
        return None
    typical = [(x['high'] + x['low'] + x['close']) / 3 for x in candles[-period:]]
    mean = sum(typical) / period
    deviation = sum(abs(x - mean) for x in typical) / period
    return 0.0 if deviation == 0 else (typical[-1] - mean) / (0.015 * deviation)


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    ranges = []
    for i in range(1, len(candles)):
        cur, prev = candles[i], candles[i - 1]['close']
        ranges.append(max(cur['high'] - cur['low'], abs(cur['high'] - prev), abs(cur['low'] - prev)))
    return sum(ranges[-period:]) / period


def adx(candles, period=14):
    if len(candles) < period + 1:
        return None
    plus, minus, trs = [], [], []
    for i in range(1, len(candles)):
        cur, prev = candles[i], candles[i - 1]
        up, down = cur['high'] - prev['high'], prev['low'] - cur['low']
        plus.append(up if up > down and up > 0 else 0.0)
        minus.append(down if down > up and down > 0 else 0.0)
        trs.append(max(cur['high'] - cur['low'], abs(cur['high'] - prev['close']), abs(cur['low'] - prev['close'])))
    tr = sum(trs[-period:])
    if tr == 0:
        return 0.0
    pdi, mdi = 100 * sum(plus[-period:]) / tr, 100 * sum(minus[-period:]) / tr
    return 0.0 if pdi + mdi == 0 else 100 * abs(pdi - mdi) / (pdi + mdi)


def candle_pattern(candles):
    if len(candles) < 2:
        return 'Недостаточно данных'
    cur, prev = candles[-1], candles[-2]
    body, span = abs(cur['close'] - cur['open']), cur['high'] - cur['low']
    if span == 0 or body / span < 0.1:
        return 'Доджи'
    if cur['close'] > cur['open'] and prev['close'] < prev['open'] and cur['close'] > prev['open'] and cur['open'] < prev['close']:
        return 'Бычье поглощение'
    if cur['close'] < cur['open'] and prev['close'] > prev['open'] and cur['close'] < prev['open'] and cur['open'] > prev['close']:
        return 'Медвежье поглощение'
    return 'Бычья свеча' if cur['close'] > cur['open'] else 'Медвежья свеча'


def analyze(candles):
    closes = [x['close'] for x in candles]
    volumes = [x['volume'] for x in candles]
    e20, e50, rv, mv = ema(closes, 20), ema(closes, 50), rsi(closes), macd(closes)
    bb, st, cv = bollinger(closes), stochastic(candles), cci(candles)
    av, dx = atr(candles), adx(candles)
    score = 0
    if e20 is not None and e50 is not None:
        score += 1 if e20 > e50 else -1 if e20 < e50 else 0
    if rv is not None:
        score += 1 if rv > 55 else -1 if rv < 45 else 0
    if mv is not None:
        score += 1 if mv > 0 else -1 if mv < 0 else 0
    if st is not None:
        score += 1 if st < 20 else -1 if st > 80 else 0
    if cv is not None:
        score += 1 if cv > 100 else -1 if cv < -100 else 0
    signal = 'BUY 🟢' if score >= 3 else 'SELL 🔴' if score <= -3 else 'WAIT 🟡'
    recent = candles[-50:]
    fib_high, fib_low = max(x['high'] for x in recent), min(x['low'] for x in recent)
    distance = fib_high - fib_low
    fib = [fib_high - distance * level for level in (0.236, 0.382, 0.5, 0.618, 0.786)]
    return {
        'signal': signal, 'score': score, 'ema20': e20, 'ema50': e50, 'rsi': rv,
        'macd': mv, 'bollinger': bb, 'stochastic': st, 'cci': cv, 'atr': av,
        'adx': dx, 'pattern': candle_pattern(candles), 'support': fib_low,
        'resistance': fib_high, 'volume_avg': sma(volumes, 20), 'fib': fib
    }


def fmt(value, digits=6):
    return 'нет данных' if value is None else f'{value:.{digits}f}'


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        'SEIRANBOT запущен.\n\n'
        'Команды:\n'
        '/price BTCUSDT — цена\n'
        '/pairs — основные пары\n'
        '/timeframes — таймфреймы\n'
        '/candles BTCUSDT 5m — последние свечи\n'
        '/analysis BTCUSDT 5m — технический анализ\n'
        '/help — помощь\n\n'
        'Бот анализирует рынок и не открывает сделки.'
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text('Пример: /price BTCUSDT')
        return
    symbol = await check_symbol(context.args[0])
    if not symbol:
        await update.message.reply_text('Пара не найдена. Пример: /price BTCUSDT')
        return
    try:
        price = await get_price(symbol)
        await update.message.reply_text(f'{symbol}\nЦена Binance: {price:.8f}')
    except Exception as exc:
        await update.message.reply_text(f'Не удалось получить цену: {exc}')


async def pairs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Основные пары:\n' + ', '.join(SYMBOLS) + '\n\nМожно запросить любую доступную спотовую USDT-пару Binance, например /price XRPUSDT.')


async def timeframes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Доступные таймфреймы:\n' + ', '.join(TIMEFRAMES) + '\n\nПример: /analysis BTCUSDT 15m')


async def candles_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text('Пример: /candles BTCUSDT 1m')
        return
    symbol = await check_symbol(context.args[0])
    interval = context.args[1] if len(context.args) > 1 else '1m'
    if not symbol:
        await update.message.reply_text('Пара не найдена или не торгуется на Binance Spot.')
        return
    try:
        candles = await get_candles(symbol, interval, 5)
        lines = [f'{symbol} — последние свечи {interval}:']
        for c in candles:
            color = '🟢' if c['close'] >= c['open'] else '🔴'
            lines.append(f"{color} O {c['open']:.8f} | H {c['high']:.8f} | L {c['low']:.8f} | C {c['close']:.8f}")
        await update.message.reply_text('\n'.join(lines))
    except Exception as exc:
        await update.message.reply_text(f'Не удалось получить свечи: {exc}')


async def analysis_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text('Пример: /analysis BTCUSDT 5m')
        return
    symbol = await check_symbol(context.args[0])
    interval = context.args[1] if len(context.args) > 1 else '1m'
    if not symbol:
        await update.message.reply_text('Пара не найдена или не торгуется на Binance Spot.')
        return
    try:
        candles = await get_candles(symbol, interval, 200)
        if len(candles) < 55:
            await update.message.reply_text('Недостаточно свечей для анализа.')
            return
        result = analyze(candles)
        price = candles[-1]['close']
        bb = result['bollinger']
        text = (
            f'АНАЛИЗ {symbol} | {interval}\n'
            f'Цена: {price:.8f}\n'
            f'Сигнал: {result["signal"]} (балл {result["score"]}/5)\n\n'
            f'EMA20: {fmt(result["ema20"])}\nEMA50: {fmt(result["ema50"])}\n'
            f'RSI14: {fmt(result["rsi"], 2)}\nMACD: {fmt(result["macd"])}\n'
            f'Bollinger верх/середина/низ: {"нет данных" if bb is None else " / ".join(fmt(x) for x in bb)}\n'
            f'Stochastic: {fmt(result["stochastic"], 2)}\nCCI: {fmt(result["cci"], 2)}\n'
            f'ATR: {fmt(result["atr"])}\nADX-показатель: {fmt(result["adx"], 2)}\n'
            f'Свечная форма: {result["pattern"]}\n'
            f'Поддержка (50 свечей): {result["support"]:.8f}\n'
            f'Сопротивление (50 свечей): {result["resistance"]:.8f}\n'
            f'Средний объём 20 свечей: {fmt(result["volume_avg"], 2)}\n'
            f'Фибо 23.6/38.2/50/61.8/78.6: ' + ' / '.join(fmt(x) for x in result['fib']) +
            '\n\nЭто техническая оценка, не гарантия движения цены. Сделки бот не открывает.'
        )
        await update.message.reply_text(text)
    except Exception as exc:
        await update.message.reply_text(f'Ошибка анализа: {exc}')


async def health(request):
    return web.Response(text='SEIRANBOT is running')


async def post_init(application: Application):
    global http, ws_task, web_runner
    http = ClientSession(timeout=ClientTimeout(total=25))
    ws_task = asyncio.create_task(websocket_worker())
    app = web.Application()
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    web_runner = web.AppRunner(app)
    await web_runner.setup()
    port = int(os.getenv('PORT', '10000'))
    site = web.TCPSite(web_runner, '0.0.0.0', port)
    await site.start()
    log.info('Health server started on port %s', port)


async def post_shutdown(application: Application):
    global http, ws_task, web_runner
    if ws_task:
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
    if web_runner:
        await web_runner.cleanup()
    if http:
        await http.close()


def main():
    if not TOKEN:
        raise RuntimeError('Не задана переменная окружения BOT_TOKEN в Render.')
    application = Application.builder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('price', price_command))
    application.add_handler(CommandHandler('pairs', pairs_command))
    application.add_handler(CommandHandler('timeframes', timeframes_command))
    application.add_handler(CommandHandler('candles', candles_command))
    application.add_handler(CommandHandler('analysis', analysis_command))
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
