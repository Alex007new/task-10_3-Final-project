import os
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

# Инициализация Faker
fake = Faker("ru_RU")

# -----------------------------
# Настройки воспроизводимости
# -----------------------------
SEED = int(os.getenv("SEED", "42"))
random.seed(SEED)
Faker.seed(SEED)

def iso_z(dt: datetime) -> str:
    """ISO-8601 в UTC с суффиксом Z (без микросекунд)."""
    return dt.replace(microsecond=0).isoformat() + "Z"

def utc_now() -> datetime:
    return datetime.utcnow()

def write_json_pretty(path: str, obj: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def write_jsonl(path: str, rows: list) -> None:
    """JSONEachRow / NDJSON: 1 JSON = 1 строка."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# Константы
CATEGORIES = [
    "🥖 Зерновые и хлебобулочные изделия",
    "🥩 Мясо, рыба, яйца и бобовые",
    "🥛 Молочные продукты",
    "🍏 Фрукты и ягоды",
    "🥦 Овощи и зелень"
]

STORE_NETWORKS = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]

REAL_CITIES = [
    {"name": "Москва", "lat": 55.7558, "lon": 37.6176},
    {"name": "Санкт-Петербург", "lat": 59.9343, "lon": 30.3351},
    {"name": "Новосибирск", "lat": 55.0302, "lon": 82.9204},
    {"name": "Екатеринбург", "lat": 56.8389, "lon": 60.6057},
    {"name": "Казань", "lat": 55.7961, "lon": 49.1064},
    {"name": "Нижний Новгород", "lat": 56.3249, "lon": 44.0059},
    {"name": "Челябинск", "lat": 55.1644, "lon": 61.4368},
    {"name": "Самара", "lat": 53.2415, "lon": 50.2212},
    {"name": "Омск", "lat": 54.9893, "lon": 73.3682},
    {"name": "Ростов-на-Дону", "lat": 47.2357, "lon": 39.7015},
    {"name": "Уфа", "lat": 54.7388, "lon": 55.9721},
    {"name": "Красноярск", "lat": 56.0106, "lon": 92.8525},
    {"name": "Воронеж", "lat": 51.6606, "lon": 39.1976},
    {"name": "Пермь", "lat": 58.0105, "lon": 56.2502},
    {"name": "Волгоград", "lat": 48.7071, "lon": 44.5169},
    {"name": "Краснодар", "lat": 45.0355, "lon": 38.9753},
    {"name": "Саратов", "lat": 51.5336, "lon": 46.0343},
    {"name": "Тюмень", "lat": 57.1535, "lon": 65.5343},
    {"name": "Тольятти", "lat": 53.5078, "lon": 49.4204},
    {"name": "Ижевск", "lat": 56.8528, "lon": 53.2116},   
    {"name": "Барнаул", "lat": 53.3543, "lon": 83.7697},
    {"name": "Ульяновск", "lat": 54.3171, "lon": 48.4025},
    {"name": "Иркутск", "lat": 52.2896, "lon": 104.2806},
    {"name": "Хабаровск", "lat": 48.4802, "lon": 135.0719},
    {"name": "Ярославль", "lat": 57.6266, "lon": 39.8938},
    {"name": "Владивосток", "lat": 43.1332, "lon": 131.9113},
    {"name": "Махачкала", "lat": 42.9699, "lon": 47.5123},
    {"name": "Томск", "lat": 56.4959, "lon": 84.9726},
    {"name": "Оренбург", "lat": 51.7682, "lon": 55.0969},
    {"name": "Кемерово", "lat": 55.3559, "lon": 86.0867},
    {"name": "Рязань", "lat": 54.6194, "lon": 39.7378},
    {"name": "Астрахань", "lat": 46.3589, "lon": 48.0551},
    {"name": "Набережные Челны", "lat": 55.7436, "lon": 52.3958},
    {"name": "Пенза", "lat": 53.2012, "lon": 44.9686},
    {"name": "Липецк", "lat": 52.6103, "lon": 39.5946},
    {"name": "Киров", "lat": 58.5966, "lon": 49.6601},
    {"name": "Чебоксары", "lat": 56.1439, "lon": 47.2489},
    {"name": "Тула", "lat": 54.1961, "lon": 37.6182},
    {"name": "Калининград", "lat": 54.7075, "lon": 20.5073},
    {"name": "Балашиха", "lat": 55.7963, "lon": 37.9382},
    {"name": "Курск", "lat": 51.7373, "lon": 36.1874},
    {"name": "Ставрополь", "lat": 45.0445, "lon": 41.9691},
    {"name": "Улан-Удэ", "lat": 51.8345, "lon": 107.5846},
    {"name": "Тверь", "lat": 56.8596, "lon": 35.9119},
    {"name": "Магнитогорск", "lat": 53.4219, "lon": 58.9798},
    {"name": "Сочи", "lat": 43.6028, "lon": 39.7342},
    {"name": "Белгород", "lat": 50.5956, "lon": 36.5873},
    {"name": "Нижний Тагил", "lat": 57.9101, "lon": 59.9813},
    {"name": "Владимир", "lat": 56.1366, "lon": 40.3966},
    {"name": "Архангельск", "lat": 64.5399, "lon": 40.5153},
    {"name": "Симферополь", "lat": 44.9572, "lon": 34.1108},
    {"name": "Севастополь", "lat": 44.6167, "lon": 33.5254},
]

# Расширенный список товаров (200 позиций)
REAL_PRODUCTS = [
    # Категория 0: Хлебобулочные (40)
    {"name": "Хлеб Бородинский", "group": CATEGORIES[0], "description": "Ржаной хлеб с кориандром, нарезанный", "price": 55.0, "unit": "шт", "manufacturer_name": "ООО «Коломенский пекарь»"},
    {"name": "Батон нарезной", "group": CATEGORIES[0], "description": "Сдобный батон из пшеничной муки высшего сорта", "price": 42.0, "unit": "шт", "manufacturer_name": "АО «Каравай»"},
    {"name": "Лаваш тонкий", "group": CATEGORIES[0], "description": "Армянский лаваш, 300 г", "price": 60.0, "unit": "шт", "manufacturer_name": "ООО «Кавказская кухня»"},
    {"name": "Пирожок с картошкой", "group": CATEGORIES[0], "description": "Печёный пирожок из дрожжевого теста", "price": 35.0, "unit": "шт", "manufacturer_name": "ООО «Домашняя выпечка»"},
    {"name": "Хлеб Дарницкий", "group": CATEGORIES[0], "description": "Ржано-пшеничный хлеб", "price": 48.0, "unit": "шт", "manufacturer_name": "АО «Хлебозавод №1»"},
    {"name": "Булочка с маком", "group": CATEGORIES[0], "description": "Сдобная булочка, посыпанная маком", "price": 25.0, "unit": "шт", "manufacturer_name": "ООО «Сдоба»"},
    {"name": "Сухари панировочные", "group": CATEGORIES[0], "description": "Для жарки и запекания, 200 г", "price": 40.0, "unit": "шт", "manufacturer_name": "ООО «Русский продукт»"},
    {"name": "Баранки ванильные", "group": CATEGORIES[0], "description": "Сушки с ванильным вкусом, 300 г", "price": 65.0, "unit": "шт", "manufacturer_name": "ООО «Крендель»"},
    {"name": "Пряники Тульские", "group": CATEGORIES[0], "description": "С начинкой, 400 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Тульский пряник»"},
    {"name": "Крекер ассорти", "group": CATEGORIES[0], "description": "Солёные крекеры, 150 г", "price": 55.0, "unit": "шт", "manufacturer_name": "ООО «Печенье»"},
    {"name": "Гречка", "group": CATEGORIES[0], "description": "Крупа гречневая ядрица, 900 г", "price": 75.0, "unit": "шт", "manufacturer_name": "ООО «Мистраль»"},
    {"name": "Рис круглозёрный", "group": CATEGORIES[0], "description": "Рис для плова, 900 г", "price": 80.0, "unit": "шт", "manufacturer_name": "АО «Агроимпорт»"},
    {"name": "Овсяные хлопья", "group": CATEGORIES[0], "description": "Геркулес, 500 г", "price": 45.0, "unit": "шт", "manufacturer_name": "ООО «Овсянка»"},
    {"name": "Макароны спагетти", "group": CATEGORIES[0], "description": "Из твёрдых сортов пшеницы, 450 г", "price": 60.0, "unit": "шт", "manufacturer_name": "ООО «Макфа»"},
    {"name": "Макароны рожки", "group": CATEGORIES[0], "description": "Классические рожки, 450 г", "price": 55.0, "unit": "шт", "manufacturer_name": "ООО «Шебекинские»"},
    {"name": "Мука пшеничная в/с", "group": CATEGORIES[0], "description": "Мука для выпечки, 2 кг", "price": 85.0, "unit": "шт", "manufacturer_name": "ООО «Мельница»"},
    {"name": "Хлебцы ржаные", "group": CATEGORIES[0], "description": "Хрустящие хлебцы, 150 г", "price": 50.0, "unit": "шт", "manufacturer_name": "ООО «Здоровое питание»"},
    {"name": "Кукурузные хлопья", "group": CATEGORIES[0], "description": "Готовый завтрак, 300 г", "price": 95.0, "unit": "шт", "manufacturer_name": "ООО «Любятово»"},
    {"name": "Мюсли с орехами", "group": CATEGORIES[0], "description": "Смесь злаков с орехами, 350 г", "price": 110.0, "unit": "шт", "manufacturer_name": "ООО «Здоровое утро»"},
    {"name": "Печенье сахарное", "group": CATEGORIES[0], "description": "К чаю, 350 г", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Кондитер»"},
    {"name": "Вафли с варёной сгущёнкой", "group": CATEGORIES[0], "description": "Вафельные коржи с начинкой, 200 г", "price": 65.0, "unit": "шт", "manufacturer_name": "ООО «Сладкоежка»"},
    {"name": "Торт Медовик", "group": CATEGORIES[0], "description": "Готовый бисквитный торт, 600 г", "price": 250.0, "unit": "шт", "manufacturer_name": "ООО «Сластёна»"},
    {"name": "Кекс столичный", "group": CATEGORIES[0], "description": "С изюмом, 300 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Кекс»"},
    {"name": "Бублик с маком", "group": CATEGORIES[0], "description": "Сдобный бублик", "price": 30.0, "unit": "шт", "manufacturer_name": "ООО «Сдоба»"},
    {"name": "Сушки простые", "group": CATEGORIES[0], "description": "Мелкие сушки, 200 г", "price": 40.0, "unit": "шт", "manufacturer_name": "ООО «Крендель»"},
    {"name": "Лаваш армянский", "group": CATEGORIES[0], "description": "Тонкий лаваш, 300 г", "price": 55.0, "unit": "шт", "manufacturer_name": "ООО «Кавказская кухня»"},
    {"name": "Пита", "group": CATEGORIES[0], "description": "Восточная лепёшка, 4 шт", "price": 70.0, "unit": "уп", "manufacturer_name": "ООО «Восток-хлеб»"},
    {"name": "Чиабатта", "group": CATEGORIES[0], "description": "Итальянский хлеб, 300 г", "price": 80.0, "unit": "шт", "manufacturer_name": "ООО «Итальянская пекарня»"},
    {"name": "Багет", "group": CATEGORIES[0], "description": "Французский багет, 250 г", "price": 45.0, "unit": "шт", "manufacturer_name": "ООО «Пекарня №1»"},
    {"name": "Булочка для хот-дога", "group": CATEGORIES[0], "description": "Сдобная булочка, 4 шт", "price": 50.0, "unit": "уп", "manufacturer_name": "ООО «Булка»"},
    {"name": "Булочка для бургера", "group": CATEGORIES[0], "description": "С кунжутом, 4 шт", "price": 60.0, "unit": "уп", "manufacturer_name": "ООО «Булка»"},
    {"name": "Хлеб зерновой", "group": CATEGORIES[0], "description": "С семечками и злаками", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Здоровый хлеб»"},
    {"name": "Хлеб бездрожжевой", "group": CATEGORIES[0], "description": "На закваске", "price": 80.0, "unit": "шт", "manufacturer_name": "ООО «Деревенский хлеб»"},
    {"name": "Кускус", "group": CATEGORIES[0], "description": "Крупа из твёрдой пшеницы, 500 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Мистраль»"},
    {"name": "Булгур", "group": CATEGORIES[0], "description": "Дроблёная пшеница, 500 г", "price": 85.0, "unit": "шт", "manufacturer_name": "ООО «Мистраль»"},
    {"name": "Полба", "group": CATEGORIES[0], "description": "Зерновая культура, 500 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Здоровое зерно»"},
    {"name": "Киноа", "group": CATEGORIES[0], "description": "Крупа, 300 г", "price": 180.0, "unit": "шт", "manufacturer_name": "ООО «Эко-продукт»"},
    {"name": "Пшено", "group": CATEGORIES[0], "description": "Крупа, 900 г", "price": 40.0, "unit": "шт", "manufacturer_name": "ООО «Крупяной двор»"},
    {"name": "Ячневая крупа", "group": CATEGORIES[0], "description": "Крупа, 700 г", "price": 35.0, "unit": "шт", "manufacturer_name": "ООО «Крупяной двор»"},
    {"name": "Перловка", "group": CATEGORIES[0], "description": "Крупа, 700 г", "price": 30.0, "unit": "шт", "manufacturer_name": "ООО «Крупяной двор»"},
    # Категория 1: Мясо, рыба, яйца и бобовые (40)
    {"name": "Филе куриное", "group": CATEGORIES[1], "description": "Охлаждённое куриное филе без кожи и костей", "price": 320.0, "unit": "кг", "manufacturer_name": "Птицефабрика «Северная»"},
    {"name": "Говядина тушёная", "group": CATEGORIES[1], "description": "Консервы, 325 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Мясной двор»"},
    {"name": "Яйцо куриное С1", "group": CATEGORIES[1], "description": "Отборное куриное яйцо, 10 шт", "price": 85.0, "unit": "уп", "manufacturer_name": "Птицефабрика «Роскар»"},
    {"name": "Лосось слабосолёный", "group": CATEGORIES[1], "description": "Филе лосося, нарезка в вакуумной упаковке, 200 г", "price": 350.0, "unit": "шт", "manufacturer_name": "Русское море"},
    {"name": "Фасоль красная в собственном соку", "group": CATEGORIES[1], "description": "Консервированная, 400 г", "price": 90.0, "unit": "шт", "manufacturer_name": "Бондюэль"},
    {"name": "Свинина шейка", "group": CATEGORIES[1], "description": "Охлаждённая, для запекания", "price": 450.0, "unit": "кг", "manufacturer_name": "Мираторг"},
    {"name": "Минтай тушка", "group": CATEGORIES[1], "description": "Замороженный, без головы", "price": 180.0, "unit": "кг", "manufacturer_name": "Дальморепродукт"},
    {"name": "Котлеты домашние", "group": CATEGORIES[1], "description": "Замороженные, 400 г", "price": 150.0, "unit": "уп", "manufacturer_name": "ООО «Мясные полуфабрикаты»"},
    {"name": "Пельмени русские", "group": CATEGORIES[1], "description": "Замороженные, 500 г", "price": 130.0, "unit": "уп", "manufacturer_name": "ООО «Сибирский гурман»"},
    {"name": "Вареники с картошкой", "group": CATEGORIES[1], "description": "Замороженные, 450 г", "price": 110.0, "unit": "уп", "manufacturer_name": "ООО «Домашние вареники»"},
    {"name": "Фарш свиной", "group": CATEGORIES[1], "description": "Охлаждённый, 500 г", "price": 200.0, "unit": "шт", "manufacturer_name": "Мираторг"},
    {"name": "Сосиски молочные", "group": CATEGORIES[1], "description": "Варёные, 400 г", "price": 140.0, "unit": "шт", "manufacturer_name": "ООО «Велком»"},
    {"name": "Ветчина из индейки", "group": CATEGORIES[1], "description": "Нарезка, 300 г", "price": 170.0, "unit": "шт", "manufacturer_name": "ООО «Индейка»"},
    {"name": "Бекон", "group": CATEGORIES[1], "description": "Сырокопчёный, 200 г", "price": 180.0, "unit": "шт", "manufacturer_name": "ООО «Мясная история»"},
    {"name": "Шашлык из свинины", "group": CATEGORIES[1], "description": "Маринованный, 500 г", "price": 320.0, "unit": "шт", "manufacturer_name": "ООО «Шашлычный двор»"},
    {"name": "Грудка куриная копчёная", "group": CATEGORIES[1], "description": "Нарезка, 300 г", "price": 210.0, "unit": "шт", "manufacturer_name": "Птицефабрика «Окская»"},
    {"name": "Куриные крылья", "group": CATEGORIES[1], "description": "Охлаждённые, 1 кг", "price": 200.0, "unit": "кг", "manufacturer_name": "Птицефабрика «Северная»"},
    {"name": "Говяжий язык", "group": CATEGORIES[1], "description": "Замороженный, 1 шт", "price": 400.0, "unit": "шт", "manufacturer_name": "АО «Мясокомбинат»"},
    {"name": "Печень куриная", "group": CATEGORIES[1], "description": "Охлаждённая, 400 г", "price": 110.0, "unit": "шт", "manufacturer_name": "Птицефабрика «Северная»"},
    {"name": "Рыбные палочки", "group": CATEGORIES[1], "description": "Замороженные, 300 г", "price": 150.0, "unit": "уп", "manufacturer_name": "ООО «Морепродукт»"},
    {"name": "Камбала тушка", "group": CATEGORIES[1], "description": "Замороженная, 1 кг", "price": 220.0, "unit": "кг", "manufacturer_name": "Дальморепродукт"},
    {"name": "Сельдь слабосолёная", "group": CATEGORIES[1], "description": "Филе кусочки, 300 г", "price": 130.0, "unit": "шт", "manufacturer_name": "ООО «Рыбный край»"},
    {"name": "Красная икра", "group": CATEGORIES[1], "description": "Икра лососёвая, 140 г", "price": 550.0, "unit": "шт", "manufacturer_name": "Русское море"},
    {"name": "Консервы шпроты", "group": CATEGORIES[1], "description": "В масле, 160 г", "price": 80.0, "unit": "шт", "manufacturer_name": "ООО «Балтийский берег»"},
    {"name": "Горох колотый", "group": CATEGORIES[1], "description": "Сухой горох, 800 г", "price": 45.0, "unit": "шт", "manufacturer_name": "ООО «Крупяной двор»"},
    {"name": "Чечевица зелёная", "group": CATEGORIES[1], "description": "Сухая, 500 г", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Здоровое зерно»"},
    {"name": "Нут", "group": CATEGORIES[1], "description": "Сухой, 500 г", "price": 80.0, "unit": "шт", "manufacturer_name": "ООО «Здоровое зерно»"},
    {"name": "Фасоль белая", "group": CATEGORIES[1], "description": "Сухая, 500 г", "price": 65.0, "unit": "шт", "manufacturer_name": "ООО «Крупяной двор»"},
    {"name": "Сардины в масле", "group": CATEGORIES[1], "description": "Консервы, 240 г", "price": 95.0, "unit": "шт", "manufacturer_name": "ООО «Рыбные консервы»"},
    {"name": "Тунец в собственном соку", "group": CATEGORIES[1], "description": "Консервы, 185 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Морепродукт»"},
    {"name": "Крабовые палочки", "group": CATEGORIES[1], "description": "Охлаждённые, 200 г", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Вичи»"},
    {"name": "Яйцо перепелиное", "group": CATEGORIES[1], "description": "10 шт", "price": 75.0, "unit": "уп", "manufacturer_name": "ИП «Перепел»"},
    {"name": "Баранина", "group": CATEGORIES[1], "description": "Охлаждённая, 1 кг", "price": 550.0, "unit": "кг", "manufacturer_name": "ООО «Фермерское мясо»"},
    {"name": "Конина", "group": CATEGORIES[1], "description": "Замороженная, 1 кг", "price": 400.0, "unit": "кг", "manufacturer_name": "АО «Табун»"},
    {"name": "Манты", "group": CATEGORIES[1], "description": "Замороженные, 500 г", "price": 180.0, "unit": "уп", "manufacturer_name": "ООО «Восточные пельмени»"},
    {"name": "Хинкали", "group": CATEGORIES[1], "description": "Замороженные, 500 г", "price": 170.0, "unit": "уп", "manufacturer_name": "ООО «Кавказская кухня»"},
    {"name": "Фрикадельки мясные", "group": CATEGORIES[1], "description": "Замороженные, 400 г", "price": 130.0, "unit": "уп", "manufacturer_name": "ООО «Мясные полуфабрикаты»"},
    {"name": "Стейк из лосося", "group": CATEGORIES[1], "description": "Охлаждённый, 300 г", "price": 450.0, "unit": "шт", "manufacturer_name": "Русское море"},
    {"name": "Мидии в масле", "group": CATEGORIES[1], "description": "Консервы, 170 г", "price": 150.0, "unit": "шт", "manufacturer_name": "ООО «Морепродукт»"},
    {"name": "Кальмары", "group": CATEGORIES[1], "description": "Замороженные тушки, 500 г", "price": 250.0, "unit": "шт", "manufacturer_name": "Дальморепродукт"},
    # Категория 2: Молочные продукты (40)
    {"name": "Молоко 3,2%", "group": CATEGORIES[2], "description": "Пастеризованное молоко, 1 л", "price": 75.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
    {"name": "Творог 5%", "group": CATEGORIES[2], "description": "Мягкий творог в ванночке, 200 г", "price": 95.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Сыр Российский", "group": CATEGORIES[2], "description": "Полутвёрдый сыр, 45% жирности, 300 г", "price": 200.0, "unit": "шт", "manufacturer_name": "Сыробогатов"},
    {"name": "Йогурт питьевой клубничный", "group": CATEGORIES[2], "description": "1,5%, 270 г", "price": 48.0, "unit": "шт", "manufacturer_name": "Danone"},
    {"name": "Кефир 2,5%", "group": CATEGORIES[2], "description": "Кефир, 1 л", "price": 70.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Ряженка", "group": CATEGORIES[2], "description": "Топлёное молоко, 500 мл", "price": 55.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
    {"name": "Сметана 20%", "group": CATEGORIES[2], "description": "Сметана, 300 г", "price": 80.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Масло сливочное 82,5%", "group": CATEGORIES[2], "description": "Крестьянское масло, 180 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Маслозавод»"},
    {"name": "Сыр Плавленый", "group": CATEGORIES[2], "description": "Дружба, 90 г", "price": 35.0, "unit": "шт", "manufacturer_name": "ООО «Сырок»"},
    {"name": "Творожный сырок", "group": CATEGORIES[2], "description": "Глазированный, ванильный, 40 г", "price": 25.0, "unit": "шт", "manufacturer_name": "ООО «Вкуснотеево»"},
    {"name": "Сливки 10%", "group": CATEGORIES[2], "description": "Для кофе, 500 мл", "price": 65.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Варенец", "group": CATEGORIES[2], "description": "Кисломолочный напиток, 500 мл", "price": 50.0, "unit": "шт", "manufacturer_name": "ООО «Молочный край»"},
    {"name": "Тан", "group": CATEGORIES[2], "description": "Кисломолочный напиток, 500 мл", "price": 60.0, "unit": "шт", "manufacturer_name": "ООО «Молочный край»"},
    {"name": "Снежок", "group": CATEGORIES[2], "description": "Сладкий кисломолочный напиток, 500 мл", "price": 55.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
    {"name": "Мороженое пломбир", "group": CATEGORIES[2], "description": "Ванильное, 500 г", "price": 130.0, "unit": "шт", "manufacturer_name": "ООО «Айсберг»"},
    {"name": "Молоко сгущённое", "group": CATEGORIES[2], "description": "С сахаром, 380 г", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Густияр»"},
    {"name": "Сыр Моцарелла", "group": CATEGORIES[2], "description": "Для пиццы, 125 г", "price": 100.0, "unit": "шт", "manufacturer_name": "ООО «Италика»"},
    {"name": "Сыр Пармезан", "group": CATEGORIES[2], "description": "Тёртый, 150 г", "price": 150.0, "unit": "шт", "manufacturer_name": "ООО «Италика»"},
    {"name": "Творог обезжиренный", "group": CATEGORIES[2], "description": "0,1%, 200 г", "price": 60.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Кефир 1%", "group": CATEGORIES[2], "description": "1 л", "price": 65.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
    {"name": "Йогурт греческий", "group": CATEGORIES[2], "description": "Натуральный, 150 г", "price": 50.0, "unit": "шт", "manufacturer_name": "Danone"},
    {"name": "Ряженка 4%", "group": CATEGORIES[2], "description": "500 мл", "price": 60.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Масло топлёное", "group": CATEGORIES[2], "description": "Для жарки, 200 г", "price": 150.0, "unit": "шт", "manufacturer_name": "ООО «Маслозавод»"},
    {"name": "Сыр Косичка", "group": CATEGORIES[2], "description": "Копчёный, 200 г", "price": 130.0, "unit": "шт", "manufacturer_name": "ООО «Сыровар»"},
    {"name": "Сыр Адыгейский", "group": CATEGORIES[2], "description": "Рассольный сыр, 250 г", "price": 110.0, "unit": "шт", "manufacturer_name": "ООО «Адыгейский сыр»"},
    {"name": "Пудинг ванильный", "group": CATEGORIES[2], "description": "Готовый десерт, 120 г", "price": 35.0, "unit": "шт", "manufacturer_name": "ООО «Десерт»"},
    {"name": "Закваска для йогурта", "group": CATEGORIES[2], "description": "Сухая, 1 пакетик", "price": 30.0, "unit": "шт", "manufacturer_name": "ООО «Молочная культура»"},
    {"name": "Сыворотка молочная", "group": CATEGORIES[2], "description": "Питьевая, 1 л", "price": 40.0, "unit": "шт", "manufacturer_name": "ООО «Молочный край»"},
    {"name": "Творожная масса с изюмом", "group": CATEGORIES[2], "description": "200 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Вкуснотеево»"},
    {"name": "Сырки глазированные", "group": CATEGORIES[2], "description": "С ванилином, 5 шт, 200 г", "price": 80.0, "unit": "уп", "manufacturer_name": "ООО «Ростагроэкспорт»"},
    {"name": "Молоко безлактозное", "group": CATEGORIES[2], "description": "1 л", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Лактозам»"},
    {"name": "Соевое молоко", "group": CATEGORIES[2], "description": "1 л", "price": 110.0, "unit": "шт", "manufacturer_name": "ООО «Альтернатива»"},
    {"name": "Козье молоко", "group": CATEGORIES[2], "description": "Пастеризованное, 1 л", "price": 150.0, "unit": "шт", "manufacturer_name": "ИП «Козье подворье»"},
    {"name": "Сметана 15%", "group": CATEGORIES[2], "description": "300 г", "price": 70.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Бифидок", "group": CATEGORIES[2], "description": "Кисломолочный напиток, 500 мл", "price": 50.0, "unit": "шт", "manufacturer_name": "Вимм-Билль-Данн"},
    {"name": "Кумыс", "group": CATEGORIES[2], "description": "Напиток из кобыльего молока, 500 мл", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Башкирский кумыс»"},
    {"name": "Айран", "group": CATEGORIES[2], "description": "Кисломолочный напиток, 500 мл", "price": 55.0, "unit": "шт", "manufacturer_name": "ООО «Молочный край»"},
    {"name": "Сыр Гауда", "group": CATEGORIES[2], "description": "Полутвёрдый, 300 г", "price": 220.0, "unit": "шт", "manufacturer_name": "ООО «Сыробогатов»"},
    {"name": "Сыр Маасдам", "group": CATEGORIES[2], "description": "Полутвёрдый, 300 г", "price": 250.0, "unit": "шт", "manufacturer_name": "ООО «Сыробогатов»"},
    {"name": "Творог 9%", "group": CATEGORIES[2], "description": "Классический, 200 г", "price": 85.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    {"name": "Сливки 35%", "group": CATEGORIES[2], "description": "Для взбивания, 200 мл", "price": 100.0, "unit": "шт", "manufacturer_name": "Простоквашино"},
    # Категория 3: Фрукты и ягоды (40)
    {"name": "Яблоки Голден", "group": CATEGORIES[3], "description": "Сладкие яблоки, 1 кг", "price": 110.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
    {"name": "Бананы", "group": CATEGORIES[3], "description": "Свежие бананы, Эквадор, 1 кг", "price": 95.0, "unit": "кг", "manufacturer_name": "Эко-Фрукты"},
    {"name": "Апельсины", "group": CATEGORIES[3], "description": "Сочные апельсины, 1 кг", "price": 120.0, "unit": "кг", "manufacturer_name": "Средиземноморский экспорт"},
    {"name": "Лимоны", "group": CATEGORIES[3], "description": "Кислые лимоны, 1 кг", "price": 80.0, "unit": "кг", "manufacturer_name": "ИП Цитрус"},
    {"name": "Груши Конференция", "group": CATEGORIES[3], "description": "Сочные груши, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
    {"name": "Мандарины", "group": CATEGORIES[3], "description": "Абхазские мандарины, 1 кг", "price": 130.0, "unit": "кг", "manufacturer_name": "Кавказские фрукты"},
    {"name": "Грейпфрут", "group": CATEGORIES[3], "description": "Красный грейпфрут, 1 шт", "price": 50.0, "unit": "шт", "manufacturer_name": "ИП Цитрус"},
    {"name": "Лайм", "group": CATEGORIES[3], "description": "Свежий лайм, 1 шт", "price": 25.0, "unit": "шт", "manufacturer_name": "ИП Цитрус"},
    {"name": "Хурма", "group": CATEGORIES[3], "description": "Королёк, 1 кг", "price": 140.0, "unit": "кг", "manufacturer_name": "Фрукты Кавказа"},
    {"name": "Гранат", "group": CATEGORIES[3], "description": "Свежий гранат, 1 шт", "price": 60.0, "unit": "шт", "manufacturer_name": "Азербайджанские фрукты"},
    {"name": "Киви", "group": CATEGORIES[3], "description": "Зелёный киви, 1 кг", "price": 200.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
    {"name": "Ананас", "group": CATEGORIES[3], "description": "Свежий ананас, 1 шт", "price": 150.0, "unit": "шт", "manufacturer_name": "Тропические фрукты"},
    {"name": "Манго", "group": CATEGORIES[3], "description": "Спелое манго, 1 шт", "price": 120.0, "unit": "шт", "manufacturer_name": "Тропические фрукты"},
    {"name": "Авокадо", "group": CATEGORIES[3], "description": "Хасс, 1 шт", "price": 90.0, "unit": "шт", "manufacturer_name": "ИП Авокадо"},
    {"name": "Персики", "group": CATEGORIES[3], "description": "Сочные персики, 1 кг", "price": 180.0, "unit": "кг", "manufacturer_name": "Кубанские фрукты"},
    {"name": "Нектарины", "group": CATEGORIES[3], "description": "Гладкие персики, 1 кг", "price": 190.0, "unit": "кг", "manufacturer_name": "Кубанские фрукты"},
    {"name": "Абрикосы", "group": CATEGORIES[3], "description": "Сладкие абрикосы, 1 кг", "price": 170.0, "unit": "кг", "manufacturer_name": "Кубанские фрукты"},
    {"name": "Слива", "group": CATEGORIES[3], "description": "Черная слива, 1 кг", "price": 100.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
    {"name": "Черешня", "group": CATEGORIES[3], "description": "Сладкая черешня, 1 кг", "price": 250.0, "unit": "кг", "manufacturer_name": "Кубанские фрукты"},
    {"name": "Вишня", "group": CATEGORIES[3], "description": "Кислая вишня, 1 кг", "price": 200.0, "unit": "кг", "manufacturer_name": "Кубанские фрукты"},
    {"name": "Клубника", "group": CATEGORIES[3], "description": "Свежая клубника, 500 г", "price": 150.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Малина", "group": CATEGORIES[3], "description": "Свежая малина, 250 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Смородина черная", "group": CATEGORIES[3], "description": "Свежая смородина, 250 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Крыжовник", "group": CATEGORIES[3], "description": "Свежий крыжовник, 250 г", "price": 80.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Черника", "group": CATEGORIES[3], "description": "Свежая черника, 150 г", "price": 110.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Голубика", "group": CATEGORIES[3], "description": "Свежая голубика, 150 г", "price": 130.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Клюква", "group": CATEGORIES[3], "description": "Свежая клюква, 250 г", "price": 100.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Брусника", "group": CATEGORIES[3], "description": "Свежая брусника, 250 г", "price": 95.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Облепиха", "group": CATEGORIES[3], "description": "Свежая облепиха, 250 г", "price": 110.0, "unit": "шт", "manufacturer_name": "ИП Ягодная поляна"},
    {"name": "Виноград кишмиш", "group": CATEGORIES[3], "description": "Сладкий без косточек, 1 кг", "price": 160.0, "unit": "кг", "manufacturer_name": "Азербайджанские фрукты"},
    {"name": "Арбуз", "group": CATEGORIES[3], "description": "Свежий арбуз, 1 шт (≈5 кг)", "price": 250.0, "unit": "шт", "manufacturer_name": "Астраханские арбузы"},
    {"name": "Дыня Торпеда", "group": CATEGORIES[3], "description": "Сладкая дыня, 1 шт (≈3 кг)", "price": 200.0, "unit": "шт", "manufacturer_name": "Узбекские дыни"},
    {"name": "Инжир", "group": CATEGORIES[3], "description": "Свежий инжир, 250 г", "price": 130.0, "unit": "шт", "manufacturer_name": "Кавказские фрукты"},
    {"name": "Финики", "group": CATEGORIES[3], "description": "Сушёные финики, 500 г", "price": 150.0, "unit": "шт", "manufacturer_name": "ООО «Сухофрукты»"},
    {"name": "Курага", "group": CATEGORIES[3], "description": "Сушёные абрикосы, 300 г", "price": 120.0, "unit": "шт", "manufacturer_name": "ООО «Сухофрукты»"},
    {"name": "Чернослив", "group": CATEGORIES[3], "description": "Сушёная слива без косточки, 300 г", "price": 100.0, "unit": "шт", "manufacturer_name": "ООО «Сухофрукты»"},
    {"name": "Изюм", "group": CATEGORIES[3], "description": "Сушёный виноград, 300 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Сухофрукты»"},
    {"name": "Яблоки Красные", "group": CATEGORIES[3], "description": "Сорт Ред Делишес, 1 кг", "price": 120.0, "unit": "кг", "manufacturer_name": "ИП Фруктовый сад"},
    {"name": "Лимон", "group": CATEGORIES[3], "description": "1 шт", "price": 15.0, "unit": "шт", "manufacturer_name": "ИП Цитрус"},
    {"name": "Помело", "group": CATEGORIES[3], "description": "Свежее помело, 1 шт", "price": 130.0, "unit": "шт", "manufacturer_name": "ИП Цитрус"},
    {"name": "Маракуйя", "group": CATEGORIES[3], "description": "Свежая маракуйя, 1 шт", "price": 40.0, "unit": "шт", "manufacturer_name": "Тропические фрукты"},
    # Категория 4: Овощи и зелень (40)
    {"name": "Картофель мытый", "group": CATEGORIES[4], "description": "Молодой картофель, мытый, 1 кг", "price": 40.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Огурцы длинноплодные", "group": CATEGORIES[4], "description": "Тепличные огурцы, гладкие, 1 кг", "price": 130.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
    {"name": "Помидоры черри", "group": CATEGORIES[4], "description": "Красные помидоры черри на ветке, 250 г", "price": 85.0, "unit": "уп", "manufacturer_name": "Белая дача"},
    {"name": "Морковь мытая", "group": CATEGORIES[4], "description": "Морковь столовая, мытая, 1 кг", "price": 45.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Лук репчатый", "group": CATEGORIES[4], "description": "Репчатый лук, 1 кг", "price": 30.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Свекла", "group": CATEGORIES[4], "description": "Столовая свекла, 1 кг", "price": 35.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Капуста белокочанная", "group": CATEGORIES[4], "description": "Свежая капуста, 1 кг", "price": 25.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
    {"name": "Перец болгарский", "group": CATEGORIES[4], "description": "Красный сладкий перец, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
    {"name": "Баклажаны", "group": CATEGORIES[4], "description": "Свежие баклажаны, 1 кг", "price": 120.0, "unit": "кг", "manufacturer_name": "Агрокультура"},
    {"name": "Кабачки", "group": CATEGORIES[4], "description": "Молодые кабачки, 1 кг", "price": 70.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Тыква", "group": CATEGORIES[4], "description": "Свежая тыква, 1 кг", "price": 40.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Редис", "group": CATEGORIES[4], "description": "Пучок редиса, 200 г", "price": 30.0, "unit": "шт", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Редька зеленая", "group": CATEGORIES[4], "description": "Свежая редька, 1 шт", "price": 50.0, "unit": "шт", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Хрен", "group": CATEGORIES[4], "description": "Корень хрена, 1 шт", "price": 40.0, "unit": "шт", "manufacturer_name": "ИП «Огородник»"},
    {"name": "Укроп свежий", "group": CATEGORIES[4], "description": "Пучок укропа", "price": 20.0, "unit": "шт", "manufacturer_name": "ИП «Зелень»"},
    {"name": "Петрушка свежая", "group": CATEGORIES[4], "description": "Пучок петрушки", "price": 20.0, "unit": "шт", "manufacturer_name": "ИП «Зелень»"},
    {"name": "Кинза свежая", "group": CATEGORIES[4], "description": "Пучок кинзы", "price": 25.0, "unit": "шт", "manufacturer_name": "ИП «Зелень»"},
    {"name": "Салат Айсберг", "group": CATEGORIES[4], "description": "Кочан салата, 300 г", "price": 60.0, "unit": "шт", "manufacturer_name": "Белая дача"},
    {"name": "Шпинат", "group": CATEGORIES[4], "description": "Свежий шпинат, 125 г", "price": 40.0, "unit": "шт", "manufacturer_name": "Белая дача"},
    {"name": "Щавель", "group": CATEGORIES[4], "description": "Свежий щавель, пучок", "price": 30.0, "unit": "шт", "manufacturer_name": "ИП «Зелень»"},
    {"name": "Лук зеленый", "group": CATEGORIES[4], "description": "Перья лука, пучок", "price": 20.0, "unit": "шт", "manufacturer_name": "ИП «Зелень»"},
    {"name": "Чеснок", "group": CATEGORIES[4], "description": "Головки чеснока, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "ИП «Огородник»"},
    {"name": "Грибы шампиньоны", "group": CATEGORIES[4], "description": "Свежие шампиньоны, 400 г", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Грибная ферма»"},
    {"name": "Грибы вешенки", "group": CATEGORIES[4], "description": "Свежие вешенки, 300 г", "price": 80.0, "unit": "шт", "manufacturer_name": "ООО «Грибная ферма»"},
    {"name": "Кукуруза в початках", "group": CATEGORIES[4], "description": "Свежая кукуруза, 1 шт", "price": 30.0, "unit": "шт", "manufacturer_name": "Агрокультура"},
    {"name": "Горошек зеленый консервированный", "group": CATEGORIES[4], "description": "400 г", "price": 60.0, "unit": "шт", "manufacturer_name": "Бондюэль"},
    {"name": "Фасоль стручковая", "group": CATEGORIES[4], "description": "Замороженная, 400 г", "price": 80.0, "unit": "шт", "manufacturer_name": "ООО «Овощной рай»"},
    {"name": "Спаржа", "group": CATEGORIES[4], "description": "Свежая спаржа, 250 г", "price": 200.0, "unit": "шт", "manufacturer_name": "ИП «Овощная экзотика»"},
    {"name": "Артишоки", "group": CATEGORIES[4], "description": "Свежие артишоки, 1 шт", "price": 80.0, "unit": "шт", "manufacturer_name": "ИП «Овощная экзотика»"},
    {"name": "Сельдерей корневой", "group": CATEGORIES[4], "description": "Корень сельдерея, 1 шт", "price": 50.0, "unit": "шт", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Имбирь", "group": CATEGORIES[4], "description": "Корень имбиря, 100 г", "price": 40.0, "unit": "шт", "manufacturer_name": "ИП «Пряности»"},
    {"name": "Ревень", "group": CATEGORIES[4], "description": "Свежий ревень, пучок", "price": 60.0, "unit": "шт", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Батат", "group": CATEGORIES[4], "description": "Сладкий картофель, 1 кг", "price": 150.0, "unit": "кг", "manufacturer_name": "ИП «Овощная экзотика»"},
    {"name": "Топинамбур", "group": CATEGORIES[4], "description": "Земляная груша, 1 кг", "price": 80.0, "unit": "кг", "manufacturer_name": "Фермерское хозяйство «Лето»"},
    {"name": "Огурцы маринованные", "group": CATEGORIES[4], "description": "Консервированные огурцы, 680 мл", "price": 90.0, "unit": "шт", "manufacturer_name": "ООО «Консервный завод»"},
    {"name": "Помидоры маринованные", "group": CATEGORIES[4], "description": "Консервированные томаты, 720 мл", "price": 100.0, "unit": "шт", "manufacturer_name": "ООО «Консервный завод»"},
    {"name": "Лечо", "group": CATEGORIES[4], "description": "Консервированное лечо, 500 мл", "price": 70.0, "unit": "шт", "manufacturer_name": "ООО «Консервный завод»"},
    {"name": "Кабачковая икра", "group": CATEGORIES[4], "description": "Консервы, 500 г", "price": 60.0, "unit": "шт", "manufacturer_name": "ООО «Консервный завод»"},
    {"name": "Оливки без косточек", "group": CATEGORIES[4], "description": "Консервированные, 300 г", "price": 110.0, "unit": "шт", "manufacturer_name": "Италика"},
    {"name": "Маслины", "group": CATEGORIES[4], "description": "Консервированные, 300 г", "price": 100.0, "unit": "шт", "manufacturer_name": "Италика"},
    {"name": "Квашеная капуста", "group": CATEGORIES[4], "description": "Квашеная капуста с морковью, 500 г", "price": 60.0, "unit": "шт", "manufacturer_name": "ИП «Соленья»"},
]

# Функция генерации КБЖУ
def generate_kbju(group):
    if group == CATEGORIES[0]:  # Хлебобулочные
        return {
            "calories": round(random.uniform(200, 350), 1),
            "protein": round(random.uniform(6, 12), 1),
            "fat": round(random.uniform(1, 6), 1),
            "carbohydrates": round(random.uniform(40, 60), 1)
        }
    elif group == CATEGORIES[1]:  # Мясо, рыба, яйца, бобовые
        return {
            "calories": round(random.uniform(120, 350), 1),
            "protein": round(random.uniform(15, 30), 1),
            "fat": round(random.uniform(2, 25), 1),
            "carbohydrates": round(random.uniform(0, 15), 1)
        }
    elif group == CATEGORIES[2]:  # Молочные
        return {
            "calories": round(random.uniform(40, 200), 1),
            "protein": round(random.uniform(3, 10), 1),
            "fat": round(random.uniform(1, 15), 1),
            "carbohydrates": round(random.uniform(3, 12), 1)
        }
    elif group == CATEGORIES[3]:  # Фрукты, ягоды
        return {
            "calories": round(random.uniform(30, 100), 1),
            "protein": round(random.uniform(0.5, 2), 1),
            "fat": round(random.uniform(0.1, 0.8), 1),
            "carbohydrates": round(random.uniform(5, 20), 1)
        }
    else:  # Овощи, зелень
        return {
            "calories": round(random.uniform(15, 60), 1),
            "protein": round(random.uniform(0.5, 3), 1),
            "fat": round(random.uniform(0.1, 0.8), 1),
            "carbohydrates": round(random.uniform(2, 10), 1)
        }

def create_directories():
    """Создаёт необходимые директории для данных."""
    os.makedirs("data/stores", exist_ok=True)
    os.makedirs("data/products", exist_ok=True)
    os.makedirs("data/customers", exist_ok=True)
    os.makedirs("data/purchases", exist_ok=True)
    os.makedirs("data/jsonl", exist_ok=True)

def generate_stores():
    """Генерирует магазины на основе сетей и сохраняет в JSON."""
    stores = []
    store_counter = 1
    for network, count in STORE_NETWORKS:
        for _ in range(count):
            store_id = f"store-{store_counter:03d}"
            city_info = random.choice(REAL_CITIES)
            city = city_info["name"]
            latitude = city_info["lat"]
            longitude = city_info["lon"]
            lat_offset = random.uniform(-0.01, 0.01)
            lon_offset = random.uniform(-0.01, 0.01)
            street = fake.street_name()
            store = {
                "store_id": store_id,
                "store_name": f"{network} — Магазин на {street}",
                "store_network": network,
                "store_type_description": f"{'Супермаркет более 200 кв.м.' if network == 'Большая Пикча' else 'Магазин у дома менее 100 кв.м.'} Входит в сеть из {count} магазинов.",
                "type": "offline",
                "categories": CATEGORIES,
                "manager": {
                    "name": fake.name(),
                    "phone": fake.phone_number(),
                    "email": fake.email()
                },
                "location": {
                    "country": "Россия",
                    "city": city,
                    "street": street,
                    "house": str(fake.building_number()),
                    "postal_code": fake.postcode(),
                    "coordinates": {
                        "latitude": round(latitude + lat_offset, 6),
                        "longitude": round(longitude + lon_offset, 6)
                    }
                },
                "opening_hours": {
                    "mon_fri": "09:00-21:00",
                    "sat": "10:00-20:00",
                    "sun": "10:00-18:00"
                },
                "accepts_online_orders": True,
                "delivery_available": True,
                "warehouse_connected": random.choice([True, False]),
                "last_inventory_date": (utc_now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
            }
            stores.append(store)
            write_json_pretty(f"data/stores/{store_id}.json", store)
            store_counter += 1
    print(f"Сгенерировано {len(stores)} магазинов")
    return stores

def generate_products():
    """Выбирает по 10 товаров из каждой категории и генерирует их с деталями."""
    # Выбор ровно по 10 уникальных товаров из каждой категории
    selected_products = []
    for category in CATEGORIES:
        cat_products = [p for p in REAL_PRODUCTS if p["group"] == category]
        random.shuffle(cat_products)
        selected_products.extend(cat_products[:10])

    products = []
    for i, real_prod in enumerate(selected_products):
        product_id = f"prd-{1000 + i}"

        # Генерация данных производителя
        manufacturer_name = real_prod["manufacturer_name"]
        website_base = manufacturer_name.lower().replace(' ', '').replace('«', '').replace('»', '').replace('ооо', '').replace('ао', '').replace('ип', '')
        website_domain = (website_base[:30] + ".ru") if website_base else fake.domain_name()

        product = {
            "id": product_id,
            "name": real_prod["name"],
            "group": real_prod["group"],
            "description": real_prod["description"],
            "kbju": generate_kbju(real_prod["group"]),
            "price": real_prod["price"],
            "unit": real_prod["unit"],
            "origin_country": "Россия",
            "expiry_days": random.randint(5, 30),
            "is_organic": random.choice([True, False]),
            "barcode": fake.ean(length=13),
            "manufacturer": {
                "name": manufacturer_name,
                "country": "Россия",
                "website": f"https://{website_domain}",
                "inn": fake.bothify(text='##########')
            }
        }
        products.append(product)
        write_json_pretty(f"data/products/{product['id']}.json", product)

    print(f"Сгенерировано {len(products)} товаров")
    return products

def generate_customers(stores):
    """Генерирует покупателей, привязанных к магазинам."""
    customers = []
    for store in stores:
        customer_id = f"cus-{1000 + len(customers)}"
        gender = random.choice(["male", "female"])
        if gender == "male":
            first_name = fake.first_name_male()
            last_name = fake.last_name_male()
        else:
            first_name = fake.first_name_female()
            last_name = fake.last_name_female()

        customer = {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": fake.email(),
            "phone": fake.phone_number(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "gender": gender,
            "registration_date": iso_z(utc_now() - timedelta(days=random.randint(0, 365))),
            "is_loyalty_member": True,
            "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}",
            "purchase_location": {
                "store_id": store["store_id"],
                "store_name": store["store_name"],
                "store_network": store["store_network"],
                "store_type_description": store["store_type_description"],
                "country": store["location"]["country"],
                "city": store["location"]["city"],
                "street": store["location"]["street"],
                "house": store["location"]["house"],
                "postal_code": store["location"]["postal_code"]
            },
            "delivery_address": {
                "country": "Россия",
                "city": store["location"]["city"],
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "apartment": str(random.randint(1, 100)),
                "postal_code": fake.postcode()
            },
            "preferences": {
                "preferred_language": "ru",
                "preferred_payment_method": random.choice(["card", "cash"]),
                "receive_promotions": random.choice([True, False])
            }
        }
        customers.append(customer)
        write_json_pretty(f"data/customers/{customer_id}.json", customer)

    print(f"Сгенерировано {len(customers)} покупателей")
    return customers

def generate_purchases(customers, stores, products, num_purchases=200):
    """Генерирует заданное количество покупок и сохраняет в JSON."""
    purchases = []
    for i in range(num_purchases):
        customer = random.choice(customers)
        # 80% покупок в "домашнем" магазине клиента
        home_store_id = customer["purchase_location"]["store_id"]
        if random.random() < 0.8:
            store = next(s for s in stores if s["store_id"] == home_store_id)
        else:
            store = random.choice(stores)
        items = random.sample(products, k=random.randint(1, 3))
        purchase_items = []
        total = 0
        for item in items:
            qty = random.randint(1, 5)
            total_price = round(item["price"] * qty, 2)
            total += total_price
            purchase_items.append({
                "product_id": item["id"],
                "name": item["name"],
                "category": item["group"],
                "quantity": qty,
                "unit": item["unit"],
                "price_per_unit": item["price"],
                "total_price": total_price,
                "kbju": item["kbju"],
                "manufacturer": item["manufacturer"]
            })
        is_delivery = random.choice([True, False])
        purchase = {
            "purchase_id": f"ord-{i+1:05d}",
            "customer": {
                "customer_id": customer["customer_id"],
                "first_name": customer["first_name"],
                "last_name": customer["last_name"],
                "email": customer["email"],
                "phone": customer["phone"],
                "is_loyalty_member": customer["is_loyalty_member"],
                "loyalty_card_number": customer["loyalty_card_number"]
            },
            "store": {
                "store_id": store["store_id"],
                "store_name": store["store_name"],
                "store_network": store["store_network"],
                "store_type_description": store["store_type_description"],
                "location": {
                    "city": store["location"]["city"],
                    "street": store["location"]["street"],
                    "house": store["location"]["house"],
                    "postal_code": store["location"]["postal_code"]
                }
            },
            "items": purchase_items,
            "total_amount": round(total, 2),
            "payment_method": random.choice(["card", "cash"]),
            "is_delivery": is_delivery,
            **({"delivery_address": customer["delivery_address"]} if is_delivery else {"delivery_address": None}),
            "purchase_datetime": iso_z(utc_now() - timedelta(days=random.randint(0, 90), minutes=random.randint(0, 1440)))
        }
        purchases.append(purchase)
        write_json_pretty(f"data/purchases/{purchase['purchase_id']}.json", purchase)

    print(f"Сгенерировано {len(purchases)} покупок")
    return purchases

def generate_all():
    """Главная функция, запускающая полную генерацию данных."""
    create_directories()
    stores = generate_stores()
    products = generate_products()
    customers = generate_customers(stores)
    purchases = generate_purchases(customers, stores, products, num_purchases=200)

    # Экспорт для Kafka topics (JSONEachRow / JSONL)
    write_jsonl("data/jsonl/stores.jsonl", stores)
    write_jsonl("data/jsonl/products.jsonl", products)
    write_jsonl("data/jsonl/customers.jsonl", customers)
    write_jsonl("data/jsonl/purchases.jsonl", purchases)

    print("Генерация данных завершена!")

if __name__ == "__main__":
    generate_all()