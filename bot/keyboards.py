from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def get_categories_navigation_keyboard(current_page, total_pages, selected_count, using_user_cats=False):
    keyboard = []

    nav_row = []
    if current_page > 0:
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"page_{current_page - 1}"))
    if current_page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"page_{current_page + 1}"))
    if nav_row:
        keyboard.append(nav_row)

    jump_row = [
        InlineKeyboardButton("🔽 -100", callback_data="jump_minus_100"),
        InlineKeyboardButton("🔼 +100", callback_data="jump_plus_100")
    ]
    keyboard.append(jump_row)

    if selected_count > 0:
        keyboard.append([InlineKeyboardButton("🚀 Анализировать", callback_data="do_analyze")])

    if using_user_cats:
        keyboard.append([InlineKeyboardButton("📋 К стандартным категориям", callback_data="switch_to_standard")])
    else:
        keyboard.append([InlineKeyboardButton("📤 К моим категориям", callback_data="switch_to_mine")])

    return InlineKeyboardMarkup(keyboard)


def get_source_selection_keyboard():
    keyboard = [
        [InlineKeyboardButton("📋 Стандартные категории", callback_data="src_standard")],
        [InlineKeyboardButton("📤 Мои категории", callback_data="src_mine")],
        [InlineKeyboardButton("🔄 Загрузить новые", callback_data="src_upload")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_after_analysis_keyboard():
    keyboard = [
        [InlineKeyboardButton("📤 Анализировать еще", callback_data="after_upload")],
        [InlineKeyboardButton("✅ Завершить", callback_data="after_start")]
    ]
    return InlineKeyboardMarkup(keyboard)
   

def get_end_keyboard():
    keyboard = [[InlineKeyboardButton("✅ Завершить", callback_data="after_end")]]
    return InlineKeyboardMarkup(keyboard)


def get_upload_result_keyboard():
    keyboard = [
        [InlineKeyboardButton("✅ Использовать этот список", callback_data="use_user_cats")],
        [InlineKeyboardButton("📋 К стандартным категориям", callback_data="src_standard")],
        [InlineKeyboardButton("🔄 Загрузить другой файл", callback_data="upload_again")]
    ]
    return InlineKeyboardMarkup(keyboard)
