import db
import os
import settings
import datetime
from time import sleep
from typing import List, Tuple

cnxn = db.DBConnection().get_connection()


def get_idrec_list(limit: int = None) -> List[int]:
    """Возвращает список idrec, которые не были распознаны."""
    if limit:
        query = f'SELECT DISTINCT TOP {limit} idRec ' \
                'FROM ClaimPages ' \
                'WHERE RecognizeStart = 0 AND FileName IS NOT NULL AND FileName <> \'\' ORDER BY idRec'
    else:
        query = 'SELECT DISTINCT idRec ' \
                'FROM ClaimPages ' \
                'WHERE RecognizeStart = 0 AND FileName IS NOT NULL AND FileName <> \'\' ORDER BY idRec'
    cursor = cnxn.execute(query)
    row = cursor.fetchone()
    res = []
    while row:
        res.append(row[0])
        row = cursor.fetchone()
    return res


def get_pages_list(id_rec: int) -> List[Tuple[int, str, str, bytes]]:
    """Возвращает список страниц книги."""
    query = 'SELECT idFile, FileName, FileType, JPGImage ' \
            'FROM ClaimPages ' \
            'WHERE idRec=? AND FileName IS NOT NULL AND FileName <> \'\''
    cursor = cnxn.execute(query, id_rec)
    row = cursor.fetchone()
    res = []
    while row:
        res.append((row[0], row[1], row[2], row[3]))
        row = cursor.fetchone()
    return res


def process_page(page: Tuple[int, str, str, bytes]):
    """Обрабатывает страницу книги."""
    output_file_path = os.path.join(settings.OUTPUT_PATH, f"{page[0]}_{page[1]}".replace(page[2], '.txt'))
    for _ in range(settings.RECOGNIZE_TIMEOUT_SECONDS * 10):
        sleep(.1)
        if os.path.exists(output_file_path):
            write_txt_to_db(output_file_path, page[0])
            os.remove(output_file_path)
            break


def write_pages_images_to_input(pages: List[Tuple[int, str, str, bytes]]) -> None:
    """Записывает изображения страниц в каталог для распознавания."""
    for page in pages:
        input_file_path = os.path.join(settings.INPUT_PATH, f"{page[0]}_{page[1]}")
        with open(input_file_path, "wb") as binary_file:
            binary_file.write(page[3])


def write_txt_to_db(file_path: str, file_id: int):
    """Записывает текст страницы в БД."""
    query = 'UPDATE ClaimPages SET TXTImage = ?, RecognizeStart = ?, RecognizeDate = ? WHERE idFile = ?'
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(file_path, mode="rb") as f:
        contents = f.read()
        cnxn.execute(query, [contents, 1, now, file_id])
        cnxn.commit()


if __name__ == '__main__':
    id_recs = get_idrec_list()
    total_books = len(id_recs)
    print(f'Total books for recognizing: {total_books}')
    i = 1
    for id_rec in id_recs:
        print(f'Processing {i}/{total_books} book (idRec = {id_rec})...')
        pages = get_pages_list(id_rec)
        total_pages = len(pages)
        write_pages_images_to_input(pages)
        j = 1
        for page in pages:
            print(f'Processing page {j}/{total_pages}, book {i}/{total_books} (idRec = {id_rec})')
            process_page(page)
            j += 1
        i += 1
