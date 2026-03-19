from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler
from config import CRITERIA_CHOICE, CRITERIA_REVENUE, CRITERIA_PRICE, CRITERIA_COMPETITORS, CRITERIA_VOLUME


async def criteria_start(update: Update, context):
    if 'criteria' not in context.user_data:
        context.user_data['criteria'] = {
            'min_revenue': 1000000,
            'max_price': 1000,
            'competitors': '2-3',
            'max_volume': 2.0
        }

    criteria = context.user_data['criteria']

    if criteria['competitors'] == 'any':
        comp_text = "не важно"
    else:
        comp_text = criteria['competitors']

    text = (
        "🔧 **Текущие настройки анализа**\n\n"
        f"📊 **Минимальная выручка:** {criteria['min_revenue']:,} руб\n"
        f"💰 **Максимальная цена:** {criteria['max_price']} руб\n"
        f"👥 **Количество конкурентов:** {comp_text}\n"
        f"📦 **Максимальный объем:** {criteria['max_volume']} л\n\n"
        "❓ Хотите изменить настройки?"
    )

    keyboard = [
        [InlineKeyboardButton("✅ Да, изменить", callback_data="change_yes")],
        [InlineKeyboardButton("❌ Нет, оставить", callback_data="change_no")]
    ]

    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return CRITERIA_CHOICE


async def criteria_choice_handler(update: Update, context):
    query = update.callback_query
    await query.answer()

    if query.data == "change_no":
        await query.edit_message_text(
            "✅ Настройки сохранены.\n\n📋 /list - выбрать категории"
        )
        return ConversationHandler.END

    elif query.data == "change_yes":
        await query.edit_message_text(
            "💰 Введите **минимальную выручку** в рублях:\n"
            "Например: 500000"
        )
        return CRITERIA_REVENUE


async def criteria_revenue_input(update: Update, context):
    try:
        value = int(update.message.text.replace(' ', ''))
        context.user_data['criteria']['min_revenue'] = value

        await update.message.reply_text(
            f"✅ Минимальная выручка: {value:,} руб\n\n"
            f"🏷️ Введите **максимальную цену** в рублях:\n"
            f"Например: 1500"
        )
        return CRITERIA_PRICE
    except:
        await update.message.reply_text("❌ Введите число")
        return CRITERIA_REVENUE


async def criteria_price_input(update: Update, context):
    try:
        value = int(update.message.text)
        context.user_data['criteria']['max_price'] = value

        await update.message.reply_text(
            f"✅ Максимальная цена: {value} руб\n\n"
            f"👥 Введите **количество конкурентов**:\n"
            f"• Диапазон: 2-3\n"
            f"• Или: не важно"
        )
        return CRITERIA_COMPETITORS
    except:
        await update.message.reply_text("❌ Введите число")
        return CRITERIA_PRICE


async def criteria_competitors_input(update: Update, context):
    text = update.message.text.lower().strip()

    if text in ['не важно', 'любое', 'any', 'нет', '0']:
        context.user_data['criteria']['competitors'] = 'any'
        comp_text = "не важно"
    else:
        try:
            text = text.replace(' ', '-')
            if '-' in text:
                parts = text.split('-')
                if len(parts) == 2:
                    min_val = int(parts[0])
                    max_val = int(parts[1])
                    if min_val <= max_val:
                        context.user_data['criteria']['competitors'] = f"{min_val}-{max_val}"
                        comp_text = f"от {min_val} до {max_val}"
                    else:
                        raise ValueError
                else:
                    raise ValueError
            else:
                val = int(text)
                context.user_data['criteria']['competitors'] = f"{val}-{val}"
                comp_text = str(val)
        except:
            await update.message.reply_text("❌ Введите диапазон (2-3) или 'не важно'")
            return CRITERIA_COMPETITORS

    await update.message.reply_text(
        f"✅ Конкуренты: {comp_text}\n\n"
        f"📦 Введите **максимальный объем** в литрах:\n"
        f"Например: 3.5"
    )
    return CRITERIA_VOLUME


async def criteria_volume_input(update: Update, context):
    try:
        value = float(update.message.text.replace(',', '.'))
        context.user_data['criteria']['max_volume'] = value

        criteria = context.user_data['criteria']

        if criteria['competitors'] == 'any':
            comp_text = "не важно"
        else:
            comp_text = criteria['competitors']

        await update.message.reply_text(
            f"✅ **Настройки сохранены!**\n\n"
            f"📊 Выручка > {criteria['min_revenue']:,} руб\n"
            f"💰 Цена ≤ {criteria['max_price']} руб\n"
            f"👥 Конкуренты: {comp_text}\n"
            f"📦 Объем ≤ {criteria['max_volume']} л\n\n"
            f"📋 /list - выбрать категории\n"
            f"📤 /upload - загрузить свои категории"
        )
        return ConversationHandler.END
    except:
        await update.message.reply_text("❌ Введите число")
        return CRITERIA_VOLUME


async def criteria_cancel(update: Update, context):
    await update.message.reply_text("❌ Настройка отменена")
    return ConversationHandler.END