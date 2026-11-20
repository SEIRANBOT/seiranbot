
            )

    return result


def rsi(values, period=14):
    if len(values) < period + 1:
        return None

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0) for x in changes[-period:]]
    losses = [max(-x, 0) for x in changes[-period:]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def analyze_candles(candles):
    if len(candles) < 30:
        return None

    closes = [c["close"] for c in candles]

    fast = ema(closes, 9)
    slow = ema(closes, 21)
    strength = rsi(closes)

    if fast is None or slow is None or strength is None:
        return None
"]
    }


# ==============================
# TELEGRAM COMMANDS: /price BTCUSDT")
