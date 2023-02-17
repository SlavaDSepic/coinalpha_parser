import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pars_db as db
from tqdm import tqdm
import concurrent.futures
import google_sheets as goosh
import time
import pycron
import logging


logging.basicConfig(
    level=logging.INFO,
    filename="parser_log.log",
    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
    )

url_recently = 'https://coinalpha.app/recently-add-list.html'
url_all_time_best = 'https://coinalpha.app/all-time-best.html'
url_today_best = 'https://coinalpha.app/today-best.html'
url_top_gainers = 'https://coinalpha.app/top-gainers.html'
url_top_losers = 'https://coinalpha.app/top-losers.html'
url_promoted = 'https://coinalpha.app/'

headers = {'User-Agent': UserAgent().chrome}
session = requests.Session()
session.headers = headers


def get_page(url):
    response = session.get(url, timeout=10)
    return response


#  Получить количество страниц категории
def get_number_of_pages(url) -> int:
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'lxml')
    number_of_pages = soup.find_all('a', class_='page-link')[-1].get('href').split('page=')[-1]
    return int(number_of_pages)


#  Получить все url адреса со страницы page_url
def get_coins_urls(page_url, promoted) -> list:
    response = session.get(page_url)
    soup = BeautifulSoup(response.text, 'lxml')
    if promoted:
        page_coins_urls = [f"https://coinalpha.app/{tag.get('href')}" for tag in soup.find('div', class_='box-body').find_all('a') if tag.get('href').startswith('token/')]
    else:
        page_coins_urls = [f"https://coinalpha.app/{tag.get('href')}" for tag in soup.find_all('a') if tag.get('href').startswith('token/')]
    return page_coins_urls


#  Собрать все url адреса коинов с категории сайта
def get_all_coins_urls(start_url, promoted) -> list:
    if promoted:
        return get_coins_urls(start_url, promoted)
    if start_url == 'https://coinalpha.app/today-best.html':
        number_of_pages = 1
    else:
        number_of_pages = get_number_of_pages(start_url)
    all_pages_urls = [f'{start_url}?page={i}' for i in range(1, number_of_pages+1)]
    all_coins_urls = []
    CONNECTIONS = 4
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_page_url = (executor.submit(get_coins_urls, page_url, promoted) for page_url in all_pages_urls)
        for future in tqdm(concurrent.futures.as_completed(future_to_page_url), total=len(all_pages_urls)):
            try:
                coin_url = future.result()
            except Exception as exc:
                logging.exception(f'{exc}')
            finally:
                all_coins_urls.extend(coin_url)
    logging.info(f'Coins urls parsed!   {len(all_coins_urls)}')
    return all_coins_urls


#  Спарсить всю информацию о коине по его url адресу
def parse_coin_page(coin_url):
    response = get_page(coin_url)
    soup = BeautifulSoup(response.text, 'lxml')
    try:
        coin_names = soup.find('div', class_='tokenInfo').h2
        coin_short_name = coin_names.text.split(' $')[-1]
        coin_names.b.decompose()
        coin_name = coin_names.text
    except Exception:
        logging.exception(f'{coin_url} ---parsing error')
        return None
    coin_domain = soup.find_all('a', class_='tokenHelpfulLink')[-1].get('href').split('/?utm')[0]
    coin_domains_other = ''
    socials_links = [tag.get('href') for tag in soup.find('div', class_='tokenSocialList').find('ul').find_all('a')]
    telegram = ''
    twitter = ''
    facebook = ''
    discord = ''
    reddit = ''
    linkedin = ''
    bitcointalk = ''
    medium = ''
    instagram = ''
    youtube = ''
    tiktok = ''
    other_social_links = []
    for link in socials_links:
        if 'https://t.me/' in link:
            telegram = link
        elif 'twitter.com/' in link:
            twitter = link
        elif 'facebook.com/' in link:
            facebook = link
        elif 'discord' in link:
            discord = link
        elif 'reddit.com/' in link:
            reddit = link
        elif 'linkedin.com' in link:
            linkedin = link
        elif 'bitcointalk' in link:
            bitcointalk = link
        elif 'medium.com' in link:
            medium = link
        elif 'instagram.com' in link:
            instagram = link
        elif 'youtube.com/' in link:
            youtube = link
        elif 'tiktok.com/' in link:
            tiktok = link
        else:
            other_social_links.append(link)
    try:
        coin_description = soup.find('div', class_='tokenDescriptionText').text.strip()
    except Exception:
        coin_description = 'No description'
    try:
        coin_audit = soup.find('p', text='Audited').parent.find('a').get('href')
    except Exception:
        coin_audit = 'Token Audit unknown'
    coin_listing_votes = soup.find('p', id='VotesForListingDetail').text
    coin_status = soup.find('p', id='VotesForListingDetail').parent.parent.find('li').text
    coin_listing_status = coin_listing_votes + ' ' + coin_status
    try:
        coin_launch = soup.find('ul', class_='moreInfo').find_all('li')[1].find_all('span')[1].text
        coin_presale_status = ''
    except Exception:
        coin_launch = ''
        coin_presale_status = 'Presale time end'
    coin = {'coin_name': coin_name,
            'coin_short_name': coin_short_name,
            'coin_url': coin_url,
            'coin_domain': coin_domain,
            'coin_domains_other': coin_domains_other,
            'telegram': telegram,
            'twitter': twitter,
            'facebook': facebook,
            'discord': discord,
            'reddit': reddit,
            'linkedin': linkedin,
            'bitcointalk': bitcointalk,
            'medium': medium,
            'instagram': instagram,
            'youtube': youtube,
            'tiktok': tiktok,
            'other_social_links': '\n'.join(other_social_links),
            'coin_description': coin_description,
            'coin_audit': coin_audit,
            'coin_listing_status': coin_listing_status,
            'coin_launch': coin_launch,
            'coin_presale_status': coin_presale_status
            }
    return coin


# Обновить данные таблиц
def update_table(coins_table, table_obj, table_update_obj, coins_list):
    session = db.connect_to_db()
    coins = set([c['coin_domain'] for c in coins_list])
    table_coins = set(db.get_table_coins(session, table_obj))
    coins_update = coins - table_coins
    db.clear_table(session, table_obj)
    db.write_all_to_table(session, table_obj, coins_list)
    db.clear_table(session, table_update_obj)
    coins_update = [db.get_coin_info(session, coins_table, c) for c in coins_update]
    db.write_all_to_table(session, table_update_obj, coins_update)


def get_all_coins(start_url, coins_table, table_obj, table_update_obj, promoted=False):
    """
    Функция собирает всю информацию о коинах выбранной категории start_url,
    записывает данные о новых коинах в таблицу coins_table
    и обновляет информацию в таблицах table_obj и table_update_object.
    Если установлен флаг promoted=True, то страница start_url должна содержать таблицу promoted
    """
    coins_urls = get_all_coins_urls(start_url, promoted)  # Получение url адресов всех коинов категории
    coins = []  # Список коинов, который будет возвращен в результате работы функции
    new_coins = []  # Список новых коинов, которых не было в таблицах коинов в БД
    CONNECTIONS = 4  # Количество потоков сбора данных
    sess = db.connect_to_db()  # Подключение к базе данных
    table_coins_urls = db.get_coins_urls(sess, coins_table)  # Получение списка url адресов коинов из таблицы коинов сайта
    urls_for_parse = set(coins_urls) - set(table_coins_urls)  # Получение списка url адресов коинов, которых еще нет в БД
    urls_for_parse = list(urls_for_parse)
    #  Многопоточное выполнение парсинга страниц коинов
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_coin_url = (executor.submit(parse_coin_page, coin_url) for coin_url in urls_for_parse)
        for future in tqdm(concurrent.futures.as_completed(future_to_coin_url), total=len(urls_for_parse)):
            try:
                coin = future.result()
                if not db.check_coin(sess, coins_table, coin['coin_domain']):
                    db.write_to_coins_table(sess, coins_table, coin)  # Запись нового коина в таблицу коинов сайта БД
                    if not db.check_coin(sess, db.Coins, coin['coin_domain']):
                        db.write_to_Coins(sess, coin)  # Запись нового коина в общую таблицу коинов БД
                new_coins.append(coin)
            except Exception as exc:
                logging.exception(f'{exc}')
            finally:
                pass
        sess.commit()  # Применение к базе данных всех изменений
    #  Добавление информации о коине в итоговый список coins
    for coin_url in coins_urls:
        try:
            coin_website = db.get_coin_website_by_url(sess, coins_table, coin_url)  # Получение домена коина по его url адресу
        except Exception as ex:
            continue
        coin = db.get_coin_info(sess, coins_table, coin_website)  # Получение полной информации о коине по его домену
        coins.append(coin)
    #  Обновление таблиц table_obj и table_update_obj
    update_table(coins_table, table_obj, table_update_obj, coins)
    return coins


def update_gs_table(table_name, coins_table):
    sh = goosh.open_table_by_name(table_name)
    sheets = {'Promoted': db.CoinalphaPromoted,
          'Promoted Update': db.CoinalphaPromotedUpdate,
          'Recently Added': db.CoinalphaRecentlyAdded,
          'Recently Added Update': db.CoinalphaRecentlyAddedUpdate,
          'All Time Best': db.CoinalphaAllTimeBest,
          'All Time Best Update': db.CoinalphaAllTimeBestUpdate,
          'Today Best': db.CoinalphaTodayBest,
          'Today Best Update': db.CoinalphaTodayBestUpdate,
          'Top Gainers': db.CoinalphaTopGainers,
          'Top Gainers Update': db.CoinalphaTopGainersUpdate,
          'Top Losers': db.CoinalphaTopLosers,
          'Top Losers Update': db.CoinalphaTopLosersUpdate
              }
    sess = db.connect_to_db()
    for ws_name, table_obj in sheets.items():
        coins = db.get_table_coins(sess, table_obj)
        coins_data = [db.get_coin_from_db(sess, coins_table, coin_domain) for coin_domain in coins]
        goosh.update_worksheet(sh, ws_name, coins_data)
        time.sleep(5)


def parse():
    get_all_coins(url_promoted, db.CoinalphaCoins, db.CoinalphaPromoted, db.CoinalphaPromotedUpdate, promoted=True)
    get_all_coins(url_recently, db.CoinalphaCoins, db.CoinalphaRecentlyAdded, db.CoinalphaRecentlyAddedUpdate, promoted=False)
    get_all_coins(url_all_time_best, db.CoinalphaCoins, db.CoinalphaAllTimeBest, db.CoinalphaAllTimeBestUpdate, promoted=False)
    get_all_coins(url_today_best, db.CoinalphaCoins, db.CoinalphaTodayBest, db.CoinalphaTodayBestUpdate, promoted=False)
    get_all_coins(url_top_gainers, db.CoinalphaCoins, db.CoinalphaTopGainers, db.CoinalphaTopGainersUpdate, promoted=False)
    get_all_coins(url_top_losers, db.CoinalphaCoins, db.CoinalphaTopLosers, db.CoinalphaTopLosersUpdate, promoted=False)
    update_gs_table('Coinalpha.app', db.CoinalphaCoins)
    pass


# Первый парсинг при запуске и далее парсинг по расписанию
if __name__ == '__main__':
    parse()
    while True:
        if pycron.is_now('20 1 * * *'):  # каждый день в 01:20
            try:
                logging.info('Start parsing!')
                parse()
                logging.info('End parsing!')
            except Exception as ex:
                logging.critical(ex, ' - Global ERROR')
            time.sleep(60)
        else:
            # проверяем раз в 15 сек таймер
            time.sleep(15)
