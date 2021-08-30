# импортируем библиотеки
from pyignite import Client
import re
from tqdm import tqdm

# установим подключение
client = Client()
client.connect('0.0.0.0', 32773)

# запишем существующий кэш в переменные
cache_country = client.get_cache('Country')
cache_city = client.get_cache('City')
cache_country_language = client.get_cache('CountryLanguage')


# введем ключ
# key = 'LKA'
key = input("для отображения данных введите ключ страны, например RUS:\n")

# найдем название старны по коду (значение)
country = None
for k,v in cache_country.scan():
    if v==key: 
        country = k
        break

# найдем все города по коду страны (значения) и выведем их значения
cities = []
for k,v in cache_city.scan():
    if v==key: 
        cities.append(k)
        
print(f"key: {key}, language: {cache_country_language.get(k)}, country name: {country},\
id cities in this country: {str(cities)}")
