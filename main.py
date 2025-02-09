import aiohttp
import asyncio
import aiosqlite
import re
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PATH = "the_sun.db"
semaphore = asyncio.Semaphore(50)
sitemap_cache = {}  # Кеш для sitemap

def count_phrase_occurrences(text, phrase):
    """Ищет точные вхождения фразы"""
    phrase = re.escape(phrase.lower())
    pattern = rf'\b{phrase}[\s\n\r.,!?;:\'\"”)]*\b'
    return len(re.findall(pattern, text.lower()))

async def init_db(db):
    """Создает таблицу в базе данных"""
    await db.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            phrase TEXT,
            count INTEGER,
            date_published TEXT
        )
    ''')
    await db.commit()

async def save_to_db(db, queue):
    """Обработчик очереди"""
    buffer = []
    while True:
        data = await queue.get()
        if data is None:
            break
        buffer.append(data)

        if len(buffer) >= 10:  # Записываем пачками по 10 записей
            await db.executemany(
                '''INSERT OR IGNORE INTO articles (url, title, phrase, count, date_published) 
                   VALUES (?, ?, ?, ?, ?)''', buffer
            )
            await db.commit()
            buffer.clear()

    if buffer:
        await db.executemany(
            '''INSERT OR IGNORE INTO articles (url, title, phrase, count, date_published) 
               VALUES (?, ?, ?, ?, ?)''', buffer
        )
        await db.commit()

async def extract_text_from_url(session, queue, article_url, phrase, last_month_seen):
    """Загружает страницу, извлекает текст и проверяет наличие фразы в нужных секциях"""
    async with semaphore:
        for attempt in range(3):  # Повторяем запрос до 3 раз при ошибках
            try:
                async with session.get(article_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5) as response:
                    if response.status == 200:
                        text = await response.text()
                        soup = BeautifulSoup(text, 'html.parser')
                        target_classes = ["article__content", "article-top-mobile__text-container"]

                        section_texts = []
                        for section in soup.find_all(class_=target_classes):
                            section_texts.append(section.get_text(separator=" ", strip=True).lower())

                        article_text = "\n".join(section_texts)
                        title = soup.title.string if soup.title else "Без заголовка"
                        date_published = extract_date(soup, article_url)

                        match = re.search(r'(\d{4})-(\d{2})', date_published)  # YYYY-MM
                        if match:
                            current_month = match.group(0)
                            if last_month_seen[0] != current_month:
                                logging.info(f"🔄 Начинаем просмотр статей за {current_month}")
                                last_month_seen[0] = current_month

                        phrase_pattern = re.compile(fr'(?<!\w){re.escape(phrase.lower())}(?!\w|$)', re.IGNORECASE)
                        occurrences = len(phrase_pattern.findall(article_text))

                        if occurrences > 0:
                            await queue.put((article_url, title, phrase, occurrences,date_published))
                            return article_url
                        return None
                    elif response.status in {429, 500}:
                        logging.warning(f"Ошибка {response.status} на {article_url}, попытка {attempt + 1}")
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logging.error(f"Ошибка загрузки {article_url}: {e}")
                await asyncio.sleep(2 ** attempt)
    return None

def extract_date(soup, url):
    """Извлекает дату публикации статьи"""
    meta_date = soup.find("meta", {"property": "article:published_time"}) or \
                soup.find("meta", {"name": "date"})

    if meta_date and meta_date.get("content"):
        return meta_date["content"][:10]  # Берем только YYYY-MM-DD

    match = re.search(r'(\d{4})/(\d{2})/(\d{2})', url)
    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}" if match else "Неизвестно"

async def get_sitemap_links(session, sitemap_url):
    """Загружает sitemap и извлекает ссылки"""
    if sitemap_url in sitemap_cache:
        return sitemap_cache[sitemap_url]

    for attempt in range(3):
        try:
            async with session.get(sitemap_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5) as response:
                if response.status == 200:
                    text = await response.text()
                    tree = ET.fromstring(text)
                    namespace = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
                    links = [elem.text for elem in tree.findall(f"{namespace}url/{namespace}loc") or
                             tree.findall(f"{namespace}sitemap/{namespace}loc")]
                    sitemap_cache[sitemap_url] = links
                    return links
                elif response.status in {429, 500}:
                    logging.warning(f"Ошибка {response.status} на {sitemap_url}, попытка {attempt + 1}")
                    await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Ошибка загрузки {sitemap_url}: {e}")
            await asyncio.sleep(2 ** attempt)

    return []

async def find_articles_in_sitemap(session, queue, start_sitemap, phrase, year_from, year_to):
    """Парсит sitemap и ищет статьи с указанной фразой"""
    sitemap_links = await get_sitemap_links(session, start_sitemap)

    # Фильтруем ссылки по году
    filtered_links = [link for link in sitemap_links if
                      (match := re.search(r'(\d{4})', link)) and year_from <= int(match.group(1)) <= year_to]

    all_article_urls = []
    for sitemap in filtered_links:
        logging.info(f"Читаем: {sitemap}")
        all_article_urls.extend(await get_sitemap_links(session, sitemap))

    tasks = [extract_text_from_url(session, queue, url, phrase,[""]) for url in all_article_urls]
    results = await asyncio.gather(*tasks)
    return [url for url in results if url is not None]


async def main():
    async with aiosqlite.connect(DB_PATH) as db:
        await init_db(db)
        queue = asyncio.Queue()
        #Задаваемые настройки
        sitemap_url = "https://www.thesun.co.uk/sitemap.xml"
        phrase = "dishy rishi"
        year_from = 2025
        year_to = 2025

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=100)) as session:
            worker_task = asyncio.create_task(save_to_db(db, queue))
            articles = await find_articles_in_sitemap(session, queue, sitemap_url, phrase, year_from, year_to)
            await queue.put(None)  # Завершаем очередь
            await worker_task

        logging.info(f"Найденные статьи: {len(articles)}")
        for url in articles:
            logging.info(url)


if __name__ == "__main__":
    asyncio.run(main())
