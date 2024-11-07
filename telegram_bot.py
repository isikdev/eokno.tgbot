import os
import aiomysql
import pandas as pd
import asyncio
from datetime import datetime
from telegram import (
    Bot,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import re

# Конфигурация API и базы данных
API_TOKEN = "7810266531:AAHbcd_YNWiCIRZwCVxqc0D2AdphHhkJkBY"
DB_CONFIG = {
    'host': 'localhost',
    'user': 'websen9w_parser',
    'password': 'FAwooxqZj!B8',
    'db': 'websen9w_parser',
    'charset': 'utf8mb4',
}

bot = Bot(token=API_TOKEN)
app = ApplicationBuilder().token(API_TOKEN).build()

# Команда /start
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ["📤 Экспорт БД в Excel"],
        ["🔍 Поиск по БД"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text("👋 Добро пожаловать! Выберите действие:", reply_markup=reply_markup)

# Функция для экспорта базы данных в Excel
async def export_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    filename = f"Database_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"
    progress_msg = await context.bot.send_message(chat_id, "📦 Начинаю экспорт данных...")

    # Подключение к базе данных через aiomysql
    try:
        pool = await aiomysql.create_pool(**DB_CONFIG, autocommit=True, minsize=1, maxsize=100)
    except Exception as e:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=progress_msg.message_id,
            text=f"❌ Не удалось подключиться к базе данных: {e}"
        )
        return

    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                query = "SELECT * FROM documents"
                await cursor.execute(query)
                rows = await cursor.fetchall()
                df = pd.DataFrame(rows)

        # Переименование столбцов
        df.rename(columns={
            'document_type': 'Тип документа подтверждения соответствия',
            'registration_number': 'Регистрационный номер',
            'valid_from': 'Действителен с',
            'valid_to': 'Действителен по',
            'certification_body': 'Орган по сертификации',
            'applicant': 'Заявитель',
            'manufacturer': 'Изготовитель',
            'product': 'Продукция',
            'tn_ved_code': 'Код ТН ВЭД',
            'compliance_requirements': 'Соответствует требованиям',
            'certificate_based_on': 'Сертификат выдан на основании',
            'additional_info': 'Дополнительная информация',
            'issue_date': 'Дата выпуска (регистрации)',
            'last_change_reason_status': 'Последняя причина изменения и статус',
            'shipping_documents': 'Отгрузочные документы'
        }, inplace=True)

        # Указываем нужный порядок столбцов
        df = df[[
            'Тип документа подтверждения соответствия',
            'Регистрационный номер',
            'Действителен с',
            'Действителен по',
            'Орган по сертификации',
            'Заявитель',
            'Изготовитель',
            'Продукция',
            'Код ТН ВЭД',
            'Соответствует требованиям',
            'Сертификат выдан на основании',
            'Дополнительная информация',
            'Дата выпуска (регистрации)',
            'Последняя причина изменения и статус',
            'Отгрузочные документы'
        ]]

        # Экспорт в Excel с установкой ширины колонок
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Результаты поиска')
            worksheet = writer.sheets['Результаты поиска']

            # Установка ширины колонок
            max_width = 100
            for column_cells in worksheet.columns:
                length = max(len(str(cell.value) if cell.value else "") for cell in column_cells)
                length = min(length, max_width)
                column_letter = column_cells[0].column_letter
                worksheet.column_dimensions[column_letter].width = length + 2

            # Форматирование дат
            date_format = 'DD.MM.YYYY'
            date_columns = ['Действителен с', 'Действителен по', 'Дата выпуска (регистрации)']
            for col_name in date_columns:
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name) + 1
                    for row_idx in range(2, len(df) + 2):
                        cell = worksheet.cell(row=row_idx, column=col_idx)
                        cell.number_format = date_format

        await context.bot.send_document(chat_id, document=open(filename, 'rb'))
        os.remove(filename)
        await context.bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text="✅ Экспорт завершен.")
    except Exception as e:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=progress_msg.message_id,
            text=f"❌ Ошибка при экспорте: {e}"
        )
    finally:
        pool.close()
        await pool.wait_closed()

# Поиск по базе данных
async def search_db_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    await context.bot.send_message(chat_id, "🔍 Введите ваш поисковый запрос:")

async def search_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    search_input = update.message.text.strip()
    if not search_input:
        await context.bot.send_message(chat_id, "❌ Пустой запрос.")
        return

    try:
        pool = await aiomysql.create_pool(**DB_CONFIG, autocommit=True, minsize=1, maxsize=100)
    except Exception as e:
        await context.bot.send_message(chat_id, f"❌ Не удалось подключиться к базе данных: {e}")
        return

    try:
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                # Проверяем, указано ли конкретное поле
                match = re.match(r'(\w+):\s*(.+)', search_input)
                if match:
                    field_alias, search_query = match.groups()
                    field_mapping = {
                        'Тип документа': 'document_type',
                        'Регистрационный номер': 'registration_number',
                        'Орган по сертификации': 'certification_body',
                        'Заявитель': 'applicant',
                        'Изготовитель': 'manufacturer',
                        'Продукция': 'product',
                        'Код ТН ВЭД': 'tn_ved_code',
                        'Соответствует требованиям': 'compliance_requirements',
                        'Сертификат выдан на основании': 'certificate_based_on',
                        'Дополнительная информация': 'additional_info',
                        'Последняя причина изменения и статус': 'last_change_reason_status',
                        'Отгрузочные документы': 'shipping_documents'
                    }
                    field = field_mapping.get(field_alias)
                    if field:
                        query = f"SELECT * FROM documents WHERE {field} LIKE %s"
                        params = ('%' + search_query + '%',)
                    else:
                        await context.bot.send_message(chat_id, f"❌ Поле '{field_alias}' не найдено. Пожалуйста, используйте корректное название поля.")
                        return
                else:
                    # Поиск по всем полям
                    search_query = search_input
                    search_columns = [
                        'document_type',
                        'registration_number',
                        'certification_body',
                        'applicant',
                        'manufacturer',
                        'product',
                        'tn_ved_code',
                        'compliance_requirements',
                        'certificate_based_on',
                        'additional_info',
                        'last_change_reason_status',
                        'shipping_documents'
                    ]
                    where_clause = " OR ".join([f"{col} LIKE %s" for col in search_columns])
                    params = ['%' + search_query + '%'] * len(search_columns)
                    query = f"SELECT * FROM documents WHERE {where_clause}"
                await cursor.execute(query, params)
                results = await cursor.fetchall()

                if results:
                    # Если результатов немного, отправляем их в сообщении
                    if len(results) <= 5:
                        for result in results:
                            message = (
                                f"📄 <b>Результат поиска:</b>\n\n"
                                f"<b>Тип документа:</b> {result['document_type']}\n"
                                f"<b>Регистрационный номер:</b> {result['registration_number']}\n"
                                f"<b>Действителен с:</b> {result['valid_from']}\n"
                                f"<b>Действителен по:</b> {result['valid_to']}\n"
                                f"<b>Орган по сертификации:</b> {result['certification_body']}\n"
                                f"<b>Заявитель:</b> {result['applicant']}\n"
                                f"<b>Изготовитель:</b> {result['manufacturer']}\n"
                                f"<b>Продукция:</b> {result['product']}\n"
                                f"<b>Код ТН ВЭД:</b> {result['tn_ved_code']}\n"
                                f"<b>Соответствует требованиям:</b> {result['compliance_requirements']}\n"
                                f"<b>Сертификат выдан на основании:</b> {result['certificate_based_on']}\n"
                                f"<b>Дополнительная информация:</b> {result['additional_info']}\n"
                                f"<b>Дата выпуска (регистрации):</b> {result['issue_date']}\n"
                                f"<b>Последняя причина изменения и статус:</b> {result['last_change_reason_status']}\n"
                                f"<b>Отгрузочные документы:</b> {result['shipping_documents']}\n"
                            )
                            await context.bot.send_message(chat_id, message, parse_mode="HTML")
                    else:
                        # Если результатов много, отправляем файл Excel
                        df = pd.DataFrame(results)
                        # Переименовываем столбцы
                        df.rename(columns={
                            'document_type': 'Тип документа подтверждения соответствия',
                            'registration_number': 'Регистрационный номер',
                            'valid_from': 'Действителен с',
                            'valid_to': 'Действителен по',
                            'certification_body': 'Орган по сертификации',
                            'applicant': 'Заявитель',
                            'manufacturer': 'Изготовитель',
                            'product': 'Продукция',
                            'tn_ved_code': 'Код ТН ВЭД',
                            'compliance_requirements': 'Соответствует требованиям',
                            'certificate_based_on': 'Сертификат выдан на основании',
                            'additional_info': 'Дополнительная информация',
                            'issue_date': 'Дата выпуска (регистрации)',
                            'last_change_reason_status': 'Последняя причина изменения и статус',
                            'shipping_documents': 'Отгрузочные документы'
                        }, inplace=True)
                        # Указываем нужный порядок столбцов
                        df = df[[
                            'Тип документа подтверждения соответствия',
                            'Регистрационный номер',
                            'Действителен с',
                            'Действителен по',
                            'Орган по сертификации',
                            'Заявитель',
                            'Изготовитель',
                            'Продукция',
                            'Код ТН ВЭД',
                            'Соответствует требованиям',
                            'Сертификат выдан на основании',
                            'Дополнительная информация',
                            'Дата выпуска (регистрации)',
                            'Последняя причина изменения и статус',
                            'Отгрузочные документы'
                        ]]
                        filename = f"Search_Results_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"
                        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name='Результаты поиска')
                            worksheet = writer.sheets['Результаты поиска']
                            # Установка ширины колонок
                            max_width = 100
                            for column_cells in worksheet.columns:
                                length = max(len(str(cell.value) if cell.value else "") for cell in column_cells)
                                length = min(length, max_width)
                                column_letter = column_cells[0].column_letter
                                worksheet.column_dimensions[column_letter].width = length + 2
                        await context.bot.send_document(chat_id, document=open(filename, 'rb'))
                        os.remove(filename)
                else:
                    await context.bot.send_message(chat_id, "❌ По вашему запросу ничего не найдено.")
    finally:
        pool.close()
        await pool.wait_closed()

# Обработчики команд и кнопок
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("export_db", export_db))
app.add_handler(CommandHandler("search", search_db_prompt))

# Обработчики для кнопок из клавиатуры
app.add_handler(MessageHandler(filters.Regex("📤 Экспорт БД в Excel"), export_db))
app.add_handler(MessageHandler(filters.Regex("🔍 Поиск по БД"), search_db_prompt))

# Обработчик для текстовых сообщений без URL
app.add_handler(MessageHandler(
    filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND & ~filters.Regex(r'https?://\S+'),
    search_db
))

# Запуск бота
if __name__ == '__main__':
    app.run_polling()
