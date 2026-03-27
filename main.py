import os
from datetime import date, datetime, timedelta
import logging
from clickhouse_driver import Client

client = Client(host="your_clickhouse_host")

INCOMING_DIR_LINUX = '/path/to/incoming/files'
INCOMING_DIR_WINDOWS = r'X:\path\to\incoming\files'

ARCHIVE_DIR_LINUX = '/path/to/archive/files'
ARCHIVE_DIR_WINDOWS = r'X:\path\to\archive\files'

# ===== Таблицы =====
PURCHASE_PRICE_TABLE = 'schema.purchase_price_table'
INCREMENT_DEFECTURE_TABLE = 'schema.increment_defect_table'

def query_increment_load(increment_click_table, end_dag_date: date):

    end_date_query = end_dag_date # Дата из контекста дага

    QUERY_DELETE_DATA = f'''
        ALTER TABLE {increment_click_table}
        DELETE WHERE DDATE >= toDate('{end_date_query}') - 3 AND DDATE < toDate('{end_date_query}')
    '''

    QUERY_UPDATE_TABLE = f'''
    INSERT INTO {increment_click_table}
    WITH
    -- 2 скаляра. Промежуток 3 дня до вчера
    toDate('{end_date_query}') - 3 AS START_DATE,
    toDate('{end_date_query}') as END_DATE,
    PRODUCT AS 
    (
        SELECT
            DISTINCT
            SIMILARS_CODE,
            I_CODE
        FROM
            source.product_dictionary sv 
        WHERE
            STM IN ('СТМ')
            AND I_CODE != '-'
    ),
    buffer AS (
        SELECT
            b.DDATE AS DDATE,
            CASE
                WHEN b.REGION_SIGN = 'ЦФО'  THEN 'ЦС'
                ELSE 'РС'
            END AS REGION,
            CASE
                WHEN b.REGION_SIGN = 'ЦФО' AND b.BUF_SIGN = 'DB APT' THEN 'ЦС_АБ'
                WHEN (b.REGION_SIGN = 'ЦФО' AND b.BUF_SIGN = 'DB') OR (b.REGION_SIGN = 'ЦФО' AND b.BUF_SIGN = 'Symphony') THEN 'ЦС'
                WHEN b.REGION_SIGN = 'ЮФО' AND b.BUF_SIGN = 'DB APT' THEN 'РС_АБ'
                ELSE 'РС'
            END AS MANAGEMENT,
            b.SIMILARS_CODE AS SIMILARS_CODE,
            ss.I_CODE AS I_CODE,
            CASE
                WHEN b.REPLENISHMENT > 0 THEN 1
                ELSE 0
            END AS FLAG_REPLENISHMENT,
            b.REPLENISHMENT AS BUFFER,
            CASE
                WHEN b.BUF_SIGN = 'DB' THEN 'ДБ'
                ELSE 'Не ДБ'
            END AS SIGN_DB
        FROM
        (
            SELECT
                b.DDATE AS DDATE,
                arrayJoin(['ЦФО','ЮФО']) AS REGION_SIGN,
                b.SIMILARS_CODE AS SIMILARS_CODE,
                multiIf(
                    REGION_SIGN = 'ЦФО', b.BUFFER_CFO,
                    REGION_SIGN = 'ЮФО', b.BUFFER_UFO, 
                    0
                ) AS REPLENISHMENT,
                b.BUF_SIGN AS BUF_SIGN
            FROM 
                source.buffer_table b
            WHERE
                DDATE >= START_DATE and DDATE < END_DATE
        ) AS b
        INNER JOIN 
            PRODUCT ss ON ss.SIMILARS_CODE = b.SIMILARS_CODE
        WHERE 
            b.REPLENISHMENT > 0
            AND b.DDATE >= START_DATE and b.DDATE < END_DATE
    ),
    --макс_дата
    max_date AS (
        SELECT
            q1.DDATE AS DDATE,
            q1.SIMILARS_CODE AS SIMILARS_CODE,
            ss.I_CODE AS I_CODE,
            q1.DEPTS AS DEPTS,
            q1.STORAGE AS STORAGE,
            q1.GOODS_GROUPS AS GOODS_GROUPS,
            SUM(q1.AMOUNT)AS AMOUNT
        FROM(
            SELECT
                toDate(r.DDATE) as DDATE,
                r.SIMILARS_CODE AS SIMILARS_CODE,
                r.DEPTS AS DEPTS,
                r.STORAGE AS STORAGE,
                r.GOODS_GROUPS AS GOODS_GROUPS,
                r.AMOUNT-r.RESERVE AS AMOUNT
            FROM
                source.rests_table r
            WHERE
                toHour(r.DDATE) >= 16
                AND r.STORAGE IN (31, 34, 36, 39)
                AND toDate(r.DDATE) >= START_DATE and toDate(r.DDATE) < END_DATE
        ) q1
        INNER JOIN 
            PRODUCT ss ON ss.SIMILARS_CODE = q1.SIMILARS_CODE
        WHERE
            q1.GOODS_GROUPS IN (7, 31, 26, 25)
        GROUP BY 1,2,3,4,5, 6
    ),
    --мин_дата
    min_date AS (
        SELECT
            q1.DDATE AS DDATE,
            q1.SIMILARS_CODE AS SIMILARS_CODE,
            ss.I_CODE AS I_CODE,
            q1.DEPTS AS DEPTS,
            q1.STORAGE AS STORAGE,
            q1.GOODS_GROUPS AS GOODS_GROUPS,
            SUM(q1.AMOUNT)AS AMOUNT
        FROM
        (
            SELECT
                toDate(r.DDATE) AS DDATE,
                r.SIMILARS_CODE AS SIMILARS_CODE,
                r.DEPTS AS DEPTS,
                r.STORAGE AS STORAGE,
                r.GOODS_GROUPS AS GOODS_GROUPS,
                r.AMOUNT-r.RESERVE AS AMOUNT
            FROM 
                source.rests_table r
            WHERE
                toHour(r.DDATE) < 16
                AND r.STORAGE IN (31,34,36, 39)
                AND toDate(r.DDATE) >= START_DATE and toDate(r.DDATE) < END_DATE
        ) q1
        INNER JOIN 
            PRODUCT ss ON ss.SIMILARS_CODE = q1.SIMILARS_CODE
        WHERE
            q1.GOODS_GROUPS IN (7, 31, 26, 25)
        GROUP BY 
            1, 2, 3, 4, 5, 6
    ),
    --джоин макс даты и мин даты
    rests_gg AS (
        SELECT
            CASE
                WHEN mad.DDATE = '1970-01-01' THEN mid.DDATE
                ELSE mad.DDATE
            END AS DDATE,
            CASE
                WHEN mad.SIMILARS_CODE = 0 THEN mid.SIMILARS_CODE
                ELSE mad.SIMILARS_CODE
            END AS SIMILARS_CODE,
            CASE
                WHEN mad.I_CODE = '' OR mad.I_CODE IS NULL THEN mid.I_CODE
                ELSE mad.I_CODE
            END AS I_CODE,
            CASE
                WHEN mad.DEPTS = 0 THEN mid.DEPTS
                ELSE mad.DEPTS
            END AS DEPTS,
            CASE
                WHEN mad.STORAGE = 0 THEN mid.STORAGE
                ELSE mad.STORAGE
            END AS STORAGE,
            CASE
                WHEN mad.GOODS_GROUPS = 0 THEN mid.GOODS_GROUPS
                ELSE mad.GOODS_GROUPS
            END AS GOODS_GROUPS,
            coalesce(mad.AMOUNT, mid.AMOUNT) as AMOUNT
        FROM 
            min_date mid
        FULL JOIN
            max_date mad ON mid.DDATE = mad.DDATE
            AND mid.SIMILARS_CODE = mad.SIMILARS_CODE
            AND mid.DEPTS = mad.DEPTS
            AND mid.STORAGE = mad.STORAGE
            AND mid.GOODS_GROUPS = mad.GOODS_GROUPS
    ),
    --агр остатки
    rest_opt_apt AS (
        SELECT
            c.DDATE AS DDATE,
            c.REGION AS REGION,
            c.SIMILARS_CODE AS SIMILARS_CODE,
            c.I_CODE AS I_CODE,
            SUM(c.AMOUNT_APT) AS AMOUNT_APT,
            SUM(c.AMOUNT_OPT) AS AMOUNT_OPT,
            SUM(c.AMOUNT_TOTAL) AS AMOUNT_TOTAL
        FROM
        (
            SELECT
                rg.DDATE AS DDATE,
                rg.I_CODE,
                CASE
                    WHEN (rg.DEPTS = 26 OR rg.DEPTS = 126) OR (rg.DEPTS = 15 AND rg.STORAGE IN (36, 39)) THEN 'РС'
                    ELSE 'ЦС'
                END REGION,
                rg.SIMILARS_CODE AS SIMILARS_CODE,
                CASE
                    WHEN rg.GOODS_GROUPS IN (7, 31, 26, 25, 44) THEN rg.AMOUNT /*А, Ф, Я, З*/
                    ELSE 0
                END AS AMOUNT_APT,
                CASE
                    WHEN rg.GOODS_GROUPS IN (7, 31, 26, 25, 44) THEN rg.AMOUNT /* А, Ф, Я, З*/
                    ELSE 0
                END AS AMOUNT_OPT,
                CASE
                    WHEN rg.GOODS_GROUPS IN (7, 31, 26, 25, 44) THEN rg.AMOUNT /*А, Ф, Я, З*/
                    ELSE 0
                END AS AMOUNT_TOTAL
            FROM rests_gg rg
        ) c
        GROUP BY 1,2,3, 4
    ),
    --------сборка остатков и буфера
    rests_buff AS (
        SELECT
            bu.DDATE AS DDATE,
            bu.MANAGEMENT AS MANAGEMENT,
            bu.SIMILARS_CODE AS SIMILARS_CODE,
            bu.I_CODE,
            bu.FLAG_REPLENISHMENT AS FLAG_REPLENISHMENT,
            bu.BUFFER AS BUFFER,
            CASE
                WHEN bu.REGION = 'ЦС' AND bu.SIGN_DB = 'Не Д' THEN ro.AMOUNT_TOTAL
                WHEN bu.REGION = 'РС' AND bu.SIGN_DB = 'Не Д' THEN ro.AMOUNT_TOTAL
                WHEN bu.REGION = 'ЦС' AND bu.SIGN_DB = 'Д' THEN ro.AMOUNT_OPT
                WHEN bu.REGION = 'РС' AND bu.SIGN_DB = 'Д' THEN ro.AMOUNT_OPT
                WHEN bu.MANAGEMENT = 'ЦС_АБ' THEN ro.AMOUNT_APT
                WHEN bu.MANAGEMENT = 'РС_АБ' THEN ro.AMOUNT_APT
                ELSE 0
            END AS AMOUNT,
            bu.SIGN_DB AS SIGN_DB
        FROM
            buffer bu
        LEFT JOIN 
            rest_opt_apt ro ON bu.DDATE = ro.DDATE
            AND bu.SIMILARS_CODE = ro.SIMILARS_CODE
            AND bu.REGION = ro.REGION
    )
    ----итоговая сборка и расчет
    SELECT
        rb.DDATE AS DDATE,
        rb.MANAGEMENT AS MANAGEMENT,
        rb.SIMILARS_CODE AS SIMILARS_CODE,
        s.CONCERNS AS CONCERNS,
        s.CONCERN_NAME AS CONCERN_NAME,
        s.MAIN_CONCERN_NAME AS MAIN_CONCERN_NAME,
        s.I_CODE AS I_CODE,
        rb.FLAG_REPLENISHMENT AS REPLENISHMENT,
        -- Для отчета ИРИС считаем деффектуру если остаток < 1/3 буффера
        CASE
            WHEN rb.AMOUNT < (rb.BUFFER / 3.0) THEN 1
            ELSE 0
        END AS NO_PRODUCT,
        rb.SIGN_DB AS SIGN_DB,
        pl.SIGN_DEFECTURE AS SIGN_DEFECTURE,
        AMOUNT,
        BUFFER,
        pp.PRICE_BUY_NONDS as PRICE_BUY_NONDS,
        pp.PRICE_BUY_SNDS AS PRICE_BUY_SNDS
    FROM
        rests_buff rb
    INNER JOIN
        schema.PRODUCT s ON rb.SIMILARS_CODE = s.SIMILARS_CODE
    -- Здесь использую интересный ASOF JOIN, он находит ближайшее соответствие текущей дате по условию. 
    -- Так как таблица с ценами СТМ нерегулярная и данные могут загрузиться в рандомнвый момент, запрос будет брать данные за последющие даты до тех пор, пока не появится новая загрузка
    -- И так далее. В других БД это делается прямо в JOIN (JOIN pp.DDATE <= rb.DDATE), в клике так не прокатывает
    ASOF LEFT JOIN
        (
            SELECT
                DDATE,
                I_CODE,
                PRICE_BUY_NONDS,
                PRICE_BUY_SNDS
            FROM 
                schema.CODE_I
            order by 
                I_CODE, DDATE
        ) pp ON pp.I_CODE = rb.I_CODE and pp.DDATE <= rb.DDATE
    LEFT JOIN (
        SELECT 
            toDate(pl.CREATE_DATE) AS DDATE,
            pl.SIMILARS_CODE AS SIMILARS_CODE,
            ifNull(pl.SIGN_DEFECTURE,0) AS SIGN_DEFECTURE
        FROM
            opt_prod.BI_PRCH_LISTS pl
        WHERE
            toDate(pl.CREATE_DATE) >= START_DATE and toDate(pl.CREATE_DATE) < END_DATE
    ) AS pl ON rb.DDATE = pl.DDATE AND rb.SIMILARS_CODE = pl.SIMILARS_CODE
    '''

    client.execute(QUERY_DELETE_DATA)
    client.execute(QUERY_UPDATE_TABLE)

def get_new_file(directory):
    '''
    Ищу последний загруженный в дирректорию файл и возвращаю полный путь к файлу

    :param directory:
    :return:
    '''
    import glob

    file = glob.glob(os.path.join(directory, '*.xlsx'))

    if not file:
        return None

    latest_file = max(file, key=os.path.getmtime)

    return latest_file

def check_new_file(directory):
    '''
    Здесь проверяю вернулся ли файл, если да, то возвращаю Таск на парсинг и загрузку в click
    Если нет, то возвращаю таск пропуска загрузки

    :param directory:
    :return:
    '''
    file = get_new_file(directory)

    if file:
        return 'load_file_from_excel_to_click'
    else:
        return 'skip_load'

def parcing_excel_file(excel_file, columns, date_from_dag):

    import pandas as pd

    dttm = date_from_dag.strftime('%Y-%m-%d %H:%M:%S')
    ddate = date_from_dag.strftime('%Y-%m-%d')

    df = pd.read_excel(
        excel_file,
        sheet_name='products_stm_only',
        engine='openpyxl',
        header=0,
        usecols=columns
    )

    if df.empty:
        raise Exception(f'Файл пустой ! Нужно проверить файл {excel_file}')

    df.dropna(subset=['Закупочная цена без НДС'], inplace=True)
    df['Закупочная цена с НДС'] = df['Закупочная цена с НДС'].fillna(0)

    df.dropna(subset=['Код товара И'], inplace=True)

    df['LOAD_DATE'] = dttm
    df['DDATE'] = ddate

    df = df.reindex(columns=['LOAD_DATE', 'DDATE', 'Код товара И', 'Закупочная цена с НДС', 'Закупочная цена без НДС'])
    df = df.rename(columns={
        'Код товара И': 'I_CODE',
        'Закупочная цена с НДС': 'PRICE_BUY_SNDS',
        'Закупочная цена без НДС': 'PRICE_BUY_NONDS'
    })

    df['LOAD_DATE'] = pd.to_datetime(df['LOAD_DATE'])
    df['DDATE'] = pd.to_datetime(df['DDATE'], format='%Y-%m-%d')

    return df

def add_file_to_archive(
        directory,
        archive_directory,
        date_from_dag: datetime
):
    '''
    Кладем новый файл в архив после загрузки в клик
    Плюс добавляю дату end_date из контекста дага

    :param directory:
    :param archive_directory:
    :param date_from_dag:
    :return:
    '''

    import shutil

    filename = get_new_file(directory=directory)

    if not filename:
        raise Exception('Файл не найден')

    file_name = os.path.basename(filename)
    name, ext = os.path.splitext(file_name)

    new_file_name = f"{name}_{date_from_dag.strftime('%Y-%m-%d_%H-%M-%S')}{ext}"
    print(new_file_name)

    archive_path = os.path.join(archive_directory, new_file_name)

    shutil.move(filename, archive_path)

    logging.info(f"Файл success перемещён в архив: {archive_path}")

def load_file_from_excel_to_click(
        directory,
        click_table_name: str,
        need_cols: list,
        date_end: datetime
):
    '''
    После парсинга инсуртим новые данные в таблицу, допом проверяем нет ли записей на ту же дату и удаляем если есть

    :param directory:
    :param click_table_name:
    :param need_cols:
    :param date_end - Это СЕГОДНЯШНЯЯ дата из контекста airflow. Ее буду использщовать в SQL и добавлять в архивный файл. Это сделано для того чтобы можно было перезапустить Даг инстанс за любой прошлый период
    :return:
    '''

    filename = get_new_file(directory)
    logging.info(f"Файл: {filename}")

    # Файл распарсился и загрузился в датафрейм
    df = parcing_excel_file(filename, columns=need_cols, date_from_dag=date_end)

    # Чтобы заинсертить эти данные перевожу в список и инсерчу в таблицу
    data = df.values.tolist()

    alter_q = f"ALTER TABLE {click_table_name} DELETE WHERE DDATE = toDate('{date_end.strftime('%Y-%m-%d')}')"
    insert_q = f'INSERT INTO {click_table_name} VALUES'

    client.execute(alter_q)
    client.execute(insert_q, data)

    logging.info(f"Загружено строк в таблицу прайсов: {len(data)}")

if os.name == 'nt':

    need_cols = ['Код товара И', 'Закупочная цена с НДС', 'Закупочная цена без НДС']

    if check_new_file(INCOMING_DIR_WINDOWS) == 'skip_load':
        query_increment_load(INCREMENT_DEFECTURE_TABLE, end_dag_date=datetime.today().strftime('%Y-%m-%d'))


else:
    from airflow import DAG
    from airflow.operators.python import PythonOperator, BranchPythonOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.trigger_rule import TriggerRule

    with DAG(
            dag_id='DEFECTURE_BY_STM_TABLE',
            default_args={
                'owner': 'Vasilchenko_N',
                'email': ['oaopt@zdravservice.ru'],
                'email_on_failure': True,
                'email_on_retry': False,
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
            },
            description='DAG обновляет таблицу дефектуры по СТМ позициям',
            schedule_interval='12 6 * * *',  # Выполнение DAG-а ежедневно (9:12) по мск
            start_date=datetime(2026, 3, 26),
            catchup=False,
            tags=['ZAKUPKI', '..OPT'],
            access_control={
                'opt_role': {'can_read', 'can_edit'}
            },
    ) as dag:

        # ==== Чекаю есть ли файл в дирректории, если нет то скипаю таск с загрузкой и архивированием, если есть паршу данные и гружу в клик ====

        t_check_file = BranchPythonOperator(
            task_id='check_exist_file',
            python_callable=check_new_file,
            op_kwargs={
                'directory': INCOMING_DIR_LINUX
            }
        )

        # ==== Грузим данные из файла с прайсами в клик ====

        t_load_from_excel_to_click = PythonOperator(
            task_id='load_file_from_excel_to_click',
            python_callable=load_file_from_excel_to_click,
            op_kwargs={
                'directory': INCOMING_DIR_LINUX,
                'click_table_name': PURCHASE_PRICE_TABLE,
                'need_cols': ['Код товара ИРИС', 'Закупочная цена с НДС', 'Закупочная цена без НДС'],
                'date_end': "{{ data_interval_end.strftime('%Y-%m-%d') }}"
            }
        )

        # ==== Перемещаем файл в архив (если он был) ====

        t_moving_file_to_archive = PythonOperator(
            task_id='moving_file_to_archive',
            python_callable=add_file_to_archive,
            op_kwargs={
                'directory': INCOMING_DIR_LINUX,
                'archive_directory': ARCHIVE_DIR_LINUX,
                'date_from_dag':  "{{ data_interval_end.strftime('%Y-%m-%d') }}"
            },
        )

        # ==== Оператор скипа таска если нового файла нет в дирректории, он ничего не делает, просто засчитывается как успешный ====

        t_skip_task = EmptyOperator(
            task_id='skip_load'
        )

        t_increment_load_to_defecture_table = PythonOperator(
            task_id='increment_load_to_defecture_table',
            python_callable=query_increment_load,
            op_kwargs={
                'increment_click_table': INCREMENT_DEFECTURE_TABLE,
                'end_dag_date': "{{ data_interval_end.strftime('%Y-%m-%d') }}"
            },
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )


        # Настройка зависимостей
        # 1 выполняется чекер файла, если он есть, тогда он возвращает таcк с загрузкой, если нет - скип таск, это возхможно благодаря BranchOperator
        # у check_file есть 2 возможных пути:
            # - load_file_from_excel_to_click
            # - skip_task
        # Дальше если файл был начнется его парсинг и загрузка в клик. Если нет - тогда скип
        # Потом инкрементальная загрузка в таблицу. Очищается 3 дня, загружаются 3 дня по вчера

        t_check_file >> [t_load_from_excel_to_click, t_skip_task]
        t_load_from_excel_to_click >> t_moving_file_to_archive
        [t_moving_file_to_archive, t_skip_task] >> t_increment_load_to_defecture_table