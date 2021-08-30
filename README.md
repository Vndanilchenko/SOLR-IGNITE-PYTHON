# SOLR-IGNITE-PYTHON
query examples of using solr and ignite with python

### SOLR
загрузка образа docker

<xml/>

    docker pull solr
    

    
запустим образ и загрузим тестовые данные в БД

<xml/>
    
    mkdir solrdata
    sudo chown 8983:8983 sorldata
    docker run -d -v "$PWD/solrdata:/var/solr" -p 8983:8983 --name my_solr solr:8 solr-precreate gettingstarted
    $ docker exec -it my_solr post -c gettingstarted example/exampledocs/manufacturers.xml


далее можно открыть админ-панель и поработать с запросами к стартовым данным

![solr admin panel](screenshots/solr%20admin%20panel.png)
 
для работы с solr из python с помощью библиотеки pysolr подготовим данные на основе статей wikipedia

<xml/>
    
    ноутбук "extract data from wikipedia.ipynb"
    

загрузим данные в solr, используя библиотеку pysolr
<xml/>

    ноутбук "solr requests.ipynb"
    

пример использования

<xml/>
   
    # импортируем библиотеки
    import pysolr
    
    # Создаем клиент solr, указывая адрес, порт и название core (можно посмотреть в Web-интерфейсе)
    solr = pysolr.Solr('http://0.0.0.0:8983/solr/gettingstarted/', always_commit=True)
    
    # Проверим доступность solr
    solr.ping()
    
    # загрузим подготовленные данные статей википедии
    with open('./wikipages_50.json', "r") as f:
        pages = json.load(f)
        
    # выполним добавление всех данных из каждой страницы
    for k,v in tqdm(pages.items()):
        solr.add([
        {
            "id": "doc_" + k,
            "title": v['meta']['title'],
            "categories": v['meta']['categories'],
            "text": v["content"]
            }])
            
    # выполним запрос, указав заголовок документа
    print("page 31 title original: ", pages["31"]["meta"]["title"])
    for res in solr.search('title:'+pages["31"]["meta"]["title"]):
        print("res title: ", res['title'])
        print("res text: \n", res['text'])
        
    # удалим все записи по ключу документа
    for k,v in tqdm(pages.items()):
        solr.delete(id="doc_"+k)



### IGNITE

запустим образ ignite с помощью docker

<xml/>

    docker run -it -p 32773:10800 -e "CONFIG_URI=https://raw.githubusercontent.com/apache/ignite/master/examples/config/example-cache.xml" apacheignite/ignite
    
![ignite docker run](screenshots/ignite%20docker%20run.png)
...
![ignite docker run ok](screenshots/ignite%20docker%20run%20ok.png)   
 
вся работа с ignite через python с помощью библиотеки pyignite описана в ноутбуке

<xml/>

    ignite requests.ipynb
    
    
примеры команд

<xml/>

    # импортируем библиотеки
    from pyignite import Client
    
    # установим подключение (порт задавали при запуске образа docker)
    client = Client()
    client.connect('0.0.0.0', 32773)
    
    # откроем файл с sql-запросом
    with open('./world.sql', 'r') as f:
        sql = f.read().splitlines()
        
    # создадаим кэш
    cache_country = client.create_cache('Country')
    
    # запишем в него данные, где ключом будет название страны, а значением ее код, предварительно распарсив данные
    for line in tqdm(sql):
        if "INSERT INTO Country(" in line:
            v, k = [i.strip() for i in re.sub("[^a-zA-Z\ \-\,]", "", line.split('VALUES')[-1]).split(',')[:2]]
            cache_country.put(k, v)
            
    # выведем все значения нашей таблицы
    result = cache_country.scan()
    
    for k, v in result:
        print(k, v)
        
    # найдем несколько значений по ключу
    cache_country.get_all(['Micronesia', 'Niger', 'Sri Lanka'])
    
##### для примера использования созданного кэша, напишем простое приложение, которое по коду страны будет показывать записанные нами данные

<xml/>

    пример кода в файле "query_to_ignite_cache.py"
    
![python ignite cache request](screenshots/python%20ignite%20cache%20request.png)   

    
##### пример работы с sql-запросами к таблицам IGNITE

<xml/>

    # Пропишем код для создания таблицы Country
    CITY_CREATE_TABLE_QUERY = '''CREATE TABLE City (
        ID INT(11),
        Name CHAR(35),
        CountryCode CHAR(3),
        District CHAR(20),
        Population INT(11),
        PRIMARY KEY (ID, CountryCode)
    ) WITH "affinityKey=CountryCode"'''
    
    # исполним sql-запрос на создание таблицы
    for query in [CITY_CREATE_TABLE_QUERY,]:
        client.sql(query)
        
    # Создадим индекс для таблицы City
    CITY_CREATE_INDEX = '''CREATE INDEX idx_country_code ON city (CountryCode)'''
    
    # выполним запрос
    for query in [CITY_CREATE_INDEX,]:
        client.sql(query)
        
    # Добавим данные в таблицу:
    city = ["INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (1,'Kabul','AFG','Kabol',1780000);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (2,'Qandahar','AFG','Qandahar',237500);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (3,'Herat','AFG','Herat',186800);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (4,'Mazar-e-Sharif','AFG','Balkh',127800);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (5,'Amsterdam','NLD','Noord-Holland',731200);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (6,'Rotterdam','NLD','Zuid-Holland',593321);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (7,'Haag','NLD','Zuid-Holland',440900);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (8,'Utrecht','NLD','Utrecht',234323);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (9,'Eindhoven','NLD','Noord-Brabant',201843);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (10,'Tilburg','NLD','Noord-Brabant',193238);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (11,'Groningen','NLD','Groningen',172701);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (12,'Breda','NLD','Noord-Brabant',160398);",
    "INSERT INTO City(ID, Name, CountryCode, District, Population) VALUES (13,'Apeldoorn','NLD','Gelderland',153491);"  
    ]
    
    # выполним запрос
    for row in city:
        client.sql(row)
        
    # напишем sql-запрос к таблице City
    query = ''' SELECT name, population FROM City where SUBSTRING(name, 2, 1) = 'a' '''
    result = client.sql(query)
    
    print('Top 10 cities with "a" on 2nd place of city name :')
    for row in result:
        print(row)
        
    
        
