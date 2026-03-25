from __future__ import annotations

import logging
import re
import time
import threading
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from cardinal import Cardinal

from FunPayAPI.common.enums import OrderStatuses
from bs4 import BeautifulSoup
import telebot
from telebot.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup as K,
    InlineKeyboardButton as B,
)

NAME = "Category Stats"
VERSION = "1.0.0"
DESCRIPTION = "Плагин добавляет новую функцию подсчет заработока с игры/категории."
CREDITS = "@kewanmov"
UUID = "d47af752-9047-4a5e-bcb2-1419ee6bf394"
SETTINGS_PAGE = False

logger = logging.getLogger("FPC.CategoryStats")

STATE_WAIT_GAME_NAME = "CS_WAIT_GAME_NAME"
CBT_REFRESH = "CS_Refresh"
CBT_NEW_SEARCH = "CS_NewSearch"

_lock = threading.Lock()
_last_results: Dict[int, dict] = {}


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt(number: float) -> str:
    num_str = f"{number:,}".replace(",", " ")
    if "." in num_str:
        integer_part, decimal_part = num_str.split(".")
        decimal_part = decimal_part.rstrip("0")
        decimal_part = f".{decimal_part}" if decimal_part else ""
    else:
        integer_part = num_str
        decimal_part = ""
    if integer_part.count(" ") == 1 and len(integer_part) == 5:
        integer_part = integer_part.replace(" ", "")
    return integer_part + decimal_part


def _fmt_price(price_dict: dict, prefix: str) -> str:
    parts = []
    for k, v in sorted(price_dict.items()):
        if k.startswith(prefix + "_"):
            symbol = k[len(prefix) + 1:]
            parts.append(f"{_fmt(round(v, 2))} {symbol}")
    return ", ".join(parts) if parts else "0 ¤"


def _detect_periods(html: str) -> list:
    try:
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div", {"class": "tc-date-left"})
        if not el:
            return ["all"]
        text = el.text.lower()
    except Exception:
        return ["all"]

    periods = ["all"]
    if any(w in text for w in (
        "час", "мин", "сек", "годин", "хвилин",
        "hour", "min", "sec", "just"
    )):
        periods.extend(["day", "week", "month"])
    elif any(w in text for w in (
        "день", "дня", "дней", "дні",
        "day", "yesterday"
    )):
        periods.extend(["week", "month"])
    elif any(w in text for w in (
        "недел", "тижд", "тижні", "week"
    )):
        periods.append("month")
    return periods


def _collect_sales(account, bot, chat_id, msg_id, query):
    pattern = re.compile(r"\b" + re.escape(query) + r"\b", re.IGNORECASE)

    sales_count = {"day": 0, "week": 0, "month": 0, "all": 0}
    sales_price = {}
    refunds_count = {"day": 0, "week": 0, "month": 0, "all": 0}
    refunds_price = {}
    categories: Dict[str, dict] = {}

    found = 0
    scanned = 0
    page = 1

    next_id, batch, locale, subcs = account.get_sales()
    if not batch and next_id is None:
        return None

    while True:
        for sale in batch:
            text = ""
            if sale.subcategory_name:
                text += sale.subcategory_name + " "
            if sale.description:
                text += sale.description + " "
            if not text.strip():
                try:
                    if sale.html:
                        text = BeautifulSoup(sale.html, "html.parser").get_text()
                except Exception:
                    pass

            if not pattern.search(text):
                continue

            try:
                curr = str(sale.currency)
            except Exception:
                curr = "?"

            found += 1
            periods = _detect_periods(sale.html) if sale.html else ["all"]

            if sale.status == OrderStatuses.REFUNDED:
                for p in periods:
                    refunds_count[p] += 1
                    refunds_price[f"{p}_{curr}"] = refunds_price.get(f"{p}_{curr}", 0) + sale.price
            else:
                for p in periods:
                    sales_count[p] += 1
                    sales_price[f"{p}_{curr}"] = sales_price.get(f"{p}_{curr}", 0) + sale.price

                cat = sale.subcategory_name or "Без категории"
                if cat not in categories:
                    categories[cat] = {}
                if curr not in categories[cat]:
                    categories[cat][curr] = {"total": 0.0, "count": 0}
                categories[cat][curr]["total"] += sale.price
                categories[cat][curr]["count"] += 1

        scanned += len(batch)

        if page % 5 == 0:
            try:
                bot.edit_message_text(
                    f"🔄 <b>Прогресс</b>\n\n"
                    f"📈 Просканировано: <code>{scanned}</code>\n"
                    f"🎯 Найдено: <code>{found}</code>",
                    chat_id, msg_id, parse_mode="HTML",
                )
            except Exception:
                pass

        if next_id is None:
            break

        ok = False
        for attempt in range(3):
            try:
                time.sleep(0.3)
                next_id, batch, locale, subcs = account.get_sales(
                    start_from=next_id, locale=locale, subcategories=subcs,
                )
                ok = True
                break
            except Exception as e:
                logger.warning(f"[CategoryStats] Попытка {attempt + 1}: {e}")
                time.sleep(1)
        if not ok:
            break

        page += 1

    for s in ("day", "week", "month", "all"):
        sales_price[s] = _fmt_price(sales_price, s)
        refunds_price[s] = _fmt_price(refunds_price, s)

    return {
        "found": found,
        "scanned": scanned,
        "categories": categories,
        "sales_count": sales_count,
        "sales_price": sales_price,
        "refunds_count": refunds_count,
        "refunds_price": refunds_price,
        "updated_at": time.strftime("%H:%M:%S"),
    }


def _build_report(query, data):
    sc = data["sales_count"]
    sp = data["sales_price"]
    rc = data["refunds_count"]
    rp = data["refunds_price"]

    if not sc["all"] and not rc["all"]:
        return (
            f"📊 Статистика по запросу <b><i>{_escape(query)}</i></b>\n\n"
            f"📈 Просканировано: <code>{_fmt(data['scanned'])}</code>\n\n"
            f"❌ Нет продаж по данному запросу."
        )

    ranked = []
    for cat, currencies in data["categories"].items():
        total = sum(d["total"] for d in currencies.values())
        count = sum(d["count"] for d in currencies.values())
        ranked.append((cat, total, count, currencies))
    ranked.sort(key=lambda x: x[1], reverse=True)

    medals = ["🥇", "🥈", "🥉"]
    cats_text = ""
    if ranked:
        lines = []
        for i, (cat, total, count, currencies) in enumerate(ranked[:5]):
            icon = medals[i] if i < 3 else f"  {i + 1}."
            price_parts = [f"{_fmt(round(d['total'], 2))} {c}" for c, d in sorted(currencies.items())]
            lines.append(f"{icon} {_escape(cat)} — <code>{count}</code> ({', '.join(price_parts)})")
        cats_text = "\n<b>Топ категорий</b>\n" + "\n".join(lines)
        if len(ranked) > 5:
            cats_text += f"\n<i>... и ещё {len(ranked) - 5}</i>"
        cats_text += "\n"

    return (
        f"📊 Статистика по запросу <b><i>{_escape(query)}</i></b>\n"
        f"\n"
        f"<b>Просканировано:</b> <code>{_fmt(data['scanned'])}</code>\n"
        f"<b>Найдено:</b> <code>{_fmt(data['found'])}</code>\n"
        f"\n"
        f"<b>Продано</b>\n"
        f"<b>За день:</b> <code>{_fmt(sc['day'])} ({sp['day']})</code>\n"
        f"<b>За неделю:</b> <code>{_fmt(sc['week'])} ({sp['week']})</code>\n"
        f"<b>За месяц:</b> <code>{_fmt(sc['month'])} ({sp['month']})</code>\n"
        f"<b>За всё время:</b> <code>{_fmt(sc['all'])} ({sp['all']})</code>\n"
        f"\n"
        f"<b>Возвращено</b>\n"
        f"<b>За день:</b> <code>{_fmt(rc['day'])} ({rp['day']})</code>\n"
        f"<b>За неделю:</b> <code>{_fmt(rc['week'])} ({rp['week']})</code>\n"
        f"<b>За месяц:</b> <code>{_fmt(rc['month'])} ({rp['month']})</code>\n"
        f"<b>За всё время:</b> <code>{_fmt(rc['all'])} ({rp['all']})</code>\n"
        f"{cats_text}\n"
        f"<i>Обновлено:</i> <code>{data['updated_at']}</code>"
    )


def _main_kb():
    kb = K()
    kb.row(
        B("🔄 Обновить", callback_data=CBT_REFRESH),
        B("🔍 Новый поиск", callback_data=CBT_NEW_SEARCH),
    )
    return kb


def _do_search(cardinal, bot, chat_id, msg_id, query):
    try:
        try:
            cardinal.account.get()
        except Exception as e:
            logger.warning(f"[CategoryStats] account.get() failed: {e}")
            bot.edit_message_text(
                f"❌ Не удалось получить данные аккаунта:\n<code>{_escape(str(e))}</code>",
                chat_id, msg_id, parse_mode="HTML",
            )
            return

        data = _collect_sales(cardinal.account, bot, chat_id, msg_id, query)
        if data is None:
            bot.edit_message_text(
                "📭 Список продаж пуст.", chat_id, msg_id, parse_mode="HTML",
            )
            return

        _last_results[chat_id] = {"data": data, "query": query}

        bot.edit_message_text(
            _build_report(query, data),
            chat_id, msg_id,
            parse_mode="HTML",
            reply_markup=_main_kb(),
        )
    except Exception as e:
        logger.error(f"[CategoryStats] Ошибка: {e}", exc_info=True)
        try:
            bot.edit_message_text(
                f"❌ Ошибка:\n<code>{_escape(str(e))}</code>",
                chat_id, msg_id, parse_mode="HTML",
            )
        except Exception:
            pass
    finally:
        _lock.release()


def init_commands(cardinal: Cardinal, *args):
    if not cardinal.telegram:
        return

    tg = cardinal.telegram
    bot = tg.bot

    def is_auth(m) -> bool:
        return m.from_user.id in tg.authorized_users

    def is_auth_cb(c) -> bool:
        return c.from_user.id in tg.authorized_users

    def cmd_category_stats(m: Message):
        if not is_auth(m):
            return
        msg = bot.send_message(
            m.chat.id,
            "🔎 <b>Поиск по продажам</b>\n"
            "\n"
            "Введите название игры, категории или ключевое слово.\n"
            "Поиск работает по названиям лотов и описаниям заказов.\n"
            "\n"
            "<b>Примеры:</b>\n"
            "├ <code>Lineage 2</code> — все продажи по игре\n"
            "├ <code>Adena</code> — конкретный товар\n"
            "├ <code>Dota 2</code> — другая игра\n"
            "└ <code>Gold</code> — общий поиск\n"
            "\n"
            "<i>⏱ Подсчёт может занять некоторое время</i>",
            parse_mode="HTML",
        )
        tg.set_state(m.chat.id, msg.id, m.from_user.id, STATE_WAIT_GAME_NAME)

    def on_text(m: Message):
        if not is_auth(m):
            return
        tg.clear_state(m.chat.id, m.from_user.id, True)
        query = m.text.strip()

        if cardinal.account is None:
            bot.send_message(m.chat.id, "❌ Бот не вошел в FunPay.", parse_mode="HTML")
            return

        if not _lock.acquire(blocking=False):
            bot.send_message(m.chat.id, "⏳ Подсчёт уже выполняется.", parse_mode="HTML")
            return

        status = bot.send_message(
            m.chat.id,
            f"🔄 Считаю статистику по запросу <b><i>{_escape(query)}</i></b>...",
            parse_mode="HTML",
        )
        threading.Thread(
            target=_do_search,
            args=(cardinal, bot, m.chat.id, status.id, query),
            daemon=True,
        ).start()

    def on_refresh(call: CallbackQuery):
        if not is_auth_cb(call):
            bot.answer_callback_query(call.id, "⛔", show_alert=True)
            return

        cached = _last_results.get(call.message.chat.id)
        if not cached:
            bot.answer_callback_query(call.id, "❌ Нет данных.", show_alert=True)
            return

        if cardinal.account is None:
            bot.answer_callback_query(call.id, "❌ Бот не вошел в FunPay.", show_alert=True)
            return

        if not _lock.acquire(blocking=False):
            bot.answer_callback_query(call.id, "⏳ Подсчёт уже выполняется.", show_alert=True)
            return

        bot.answer_callback_query(call.id, "🔄 Обновляю...")
        query = cached["query"]

        try:
            bot.edit_message_text(
                f"🔄 Обновляю статистику по запросу <b><i>{_escape(query)}</i></b>...",
                call.message.chat.id, call.message.id,
                parse_mode="HTML",
            )
        except Exception:
            pass

        threading.Thread(
            target=_do_search,
            args=(cardinal, bot, call.message.chat.id, call.message.id, query),
            daemon=True,
        ).start()

    def on_new_search(call: CallbackQuery):
        if not is_auth_cb(call):
            bot.answer_callback_query(call.id, "⛔", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        try:
            bot.edit_message_text(
                "🔎 <b>Поиск по продажам</b>\n"
                "\n"
                "Введите название игры, категории или ключевое слово.\n"
                "Поиск работает по названиям лотов и описаниям заказов.\n"
                "\n"
                "<b>Примеры:</b>\n"
                "├ <code>Lineage 2</code> — все продажи по игре\n"
                "├ <code>Adena</code> — конкретный товар\n"
                "├ <code>Dota 2</code> — другая игра\n"
                "└ <code>Gold</code> — общий поиск\n"
                "\n"
                "<i>⏱ Подсчёт может занять некоторое время</i>",
                call.message.chat.id, call.message.id,
                parse_mode="HTML",
            )
        except Exception:
            pass

        tg.set_state(
            call.message.chat.id, call.message.id,
            call.from_user.id, STATE_WAIT_GAME_NAME,
        )

    tg.cbq_handler(on_refresh, lambda c: c.data == CBT_REFRESH)
    tg.cbq_handler(on_new_search, lambda c: c.data == CBT_NEW_SEARCH)
    tg.msg_handler(cmd_category_stats, commands=["category_stats"])
    tg.msg_handler(
        on_text,
        func=lambda m: tg.check_state(m.chat.id, m.from_user.id, STATE_WAIT_GAME_NAME),
    )

    cardinal.add_telegram_commands(UUID, [
        ("category_stats", "Статистика заработка по категориям", True),
    ])


BIND_TO_PRE_INIT = [init_commands]
BIND_TO_NEW_MESSAGE = []
BIND_TO_DELETE = None
