import gspread
from config import GSHEET_TOKEN


sheets = ['Promoted',
          'Promoted Update',
          'Recently Added',
          'Recently Added Update',
          'All Time Best',
          'All Time Best Update',
          'Today Best',
          'Today Best Update',
          'Top Gainers',
          'Top Gainers Update',
          'Top Losers',
          'Top Losers Update'
          ]


def create_new_table(table_name, sheets):
    token = GSHEET_TOKEN
    sa = gspread.service_account(filename=token)
    sh = sa.create(table_name)

    # если нужно выдать доступ одному человеку
    # sh.share(email_address='your@email.com', perm_type='user', role='writer', notify=False)

    # если нужно выдать доступ всем
    sh.share(email_address='', perm_type='anyone', role='writer', notify=False)
    print(sh.url)
    with open('tables.txt', 'a') as file:
        string = f'TABLE {table_name} : {sh.url}\n'
        file.write(string)
    for sheet_name in sheets:
        create_worksheet(sh, sheet_name)
    return sh


# Удаление таблицы по id
def delete_table(table_id):
    token = GSHEET_TOKEN
    sa = gspread.service_account(filename=token)
    sa.del_spreadsheet(table_id)


# Открытие таблицы по имени, если таблицы с таким именем не существует, то создается новая
def open_table_by_name(table_name):
    token = GSHEET_TOKEN
    sa = gspread.service_account(filename=token)
    try:
        sh = sa.open(table_name)
        return sh
    except Exception as ex:
        sh = create_new_table(table_name, sheets)
        return sh


# Открытие таблицы по id
def open_table_by_id(table_id):
    token = GSHEET_TOKEN
    sa = gspread.service_account(filename=token)
    try:
        sh = sa.open_by_key(table_id)
        return sh
    except Exception as ex:
        print(ex)
        return None


# Создание нового листа в таблице
def create_worksheet(sh, ws_name):
    ws = sh.add_worksheet(title=ws_name, rows="1000", cols="23")
    return ws


# Переключение на лист с названием ws_name
def get_worksheet(sh, ws_name):
    try:
        return sh.worksheet(ws_name)
    except Exception:
        return None


# Удаление листа с названием ws_name
def delete_worksheet(sh, ws_name):
    try:
        sh.del_worksheet(sh.worksheet(ws_name))
        return f'Worksheet {ws_name} - deleted!'
    except Exception as ex:
        print(ex)
        return 'Deleting error!'


# Создание заголовков
def create_headers(ws):
    ws.update('A1:V1', [['Coin name',
                         'Coin short name',
                         'Coin url',
                         'Project domain address',
                         'Project domains other',
                         'Telegram',
                         'Twitter',
                         'Facebook',
                         'Discord',
                         'Reddit',
                         'Linkedin',
                         'Bitcointalk',
                         'Medium',
                         'Instagram',
                         'Youtube',
                         'TikTok',
                         'Other social links',
                         'Project description',
                         'Audit',
                         'Listing status',
                         'Launch',
                         'Presale Status:']])


# Заполнение одного ряда данными
def fill_row(ws, row_numb, data):
    ws.update(f'A{row_numb}:V{row_numb}', [[v for v in data.values()]])


# Очистить полностью лист
def clear_worksheet(sh, ws_name):
    try:
        ws = get_worksheet(sh, ws_name)
        rows_count = len(ws.get_values())
        ws.update(f'A1:V{rows_count+1}', [['' for _ in range(22)] for _ in range(rows_count)])
        return ws
    except Exception:
        ws = create_worksheet(sh, ws_name)
        return ws


# Получить количество заполненных строк
def get_col_val(ws, col_numb):
    values_list = ws.col_values(col_numb)
    return values_list


# Обновить все данные листа
def update_worksheet(sh, ws_name, data):
    # print(*data[:10], sep='\n')
    ws = clear_worksheet(sh, ws_name)
    create_headers(ws)
    rows_numb = len(data)
    ws.update(f'A2:V{rows_numb+1}', data)
    return ws
