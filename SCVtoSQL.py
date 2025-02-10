import csv
import psycopg2


#функция создания структуры базыданных SQL
def crt_SQL(dbname_name,user_name,password_name,host_name):
    print("Запуск SQL модуля")
    conn_SQL =  psycopg2.connect(dbname=dbname_name, user=user_name, password=password_name, host=host_name, port="5432")
    cursor = conn_SQL.cursor()
# Создание Farm_markets
    cursor.execute('''
       CREATE TABLE IF NOT EXISTS Farm_markets (
       ID SERIAL PRIMARY KEY ,
       FMID INTEGER NOT NULL,
       Season1Date VARCHAR(200) NULL ,
       Season1Time VARCHAR(200) NULL ,
       Season2Date VARCHAR(200) NULL ,
       Season2Time VARCHAR(200) NULL ,
       Season3Date VARCHAR(200) NULL ,
       Season3Time VARCHAR(200) NULL ,
       Season4Date VARCHAR(200) NULL ,
       Season4Time VARCHAR(200) NULL ,
       x VARCHAR(200) NULL,
       y VARCHAR(200) NULL,
       Location VARCHAR(200) NULL,
       Credit VARCHAR(50) NULL  ,
       WIC VARCHAR(50) NULL ,
       WICcash VARCHAR(50) NULL,
       SFMNP VARCHAR(50) NULL,
       SNAP VARCHAR(50) NULL,
       Organic VARCHAR(50) NULL,
       Bakedgoods VARCHAR(50) NULL,
       Cheese VARCHAR(50) NULL,
       Crafts VARCHAR(50) NULL,
       Flowers VARCHAR(50) NULL,
       Eggs VARCHAR(50) NULL,
       Seafood VARCHAR(50) NULL,
       Herbs VARCHAR(50) NULL,
       Vegetables VARCHAR(50) NULL,
       Honey VARCHAR(50) NULL,
       Jams VARCHAR(50) NULL,
       Maple VARCHAR(50) NULL,
       Meat VARCHAR(50) NULL,
       Nursery VARCHAR(50) NULL,
       Nuts VARCHAR(50) NULL,
       Plants VARCHAR(50) NULL,
       Poultry VARCHAR(50) NULL,
       Prepared VARCHAR(50) NULL,
       Soap VARCHAR(50) NULL,
       Trees VARCHAR(50) NULL,
       Wine VARCHAR(50) NULL,
       Coffee VARCHAR(50) NULL,
       Beans VARCHAR(50) NULL,
       Fruits VARCHAR(50) NULL,
       Grains VARCHAR(50) NULL,
       Juices VARCHAR(50) NULL,
       Mushrooms VARCHAR(50) NULL,
       PetFood VARCHAR(50) NULL,
       Tofu VARCHAR(50) NULL,
       WildHarvested VARCHAR(50) NULL,
       Update_Time VARCHAR(50) 
       );
       ''')
    cursor.execute("SELECT to_regclass('public.Farm_markets');")
    result = cursor.fetchone()
    if result is not None:
       print("Таблица Farm_markets создана успешно ")
    else:
       print("Ошибка создания таблицы Farm_markets ")
# Создание Markets
    cursor.execute('''
       CREATE TABLE IF NOT EXISTS Markets_info (
       FMID INTEGER PRIMARY KEY ,
       MarketName VARCHAR(200) NULL ,
       Street VARCHAR(200) NULL ,
       City VARCHAR(200) NULL ,
       Country VARCHAR(200) NULL ,
       State_US VARCHAR(200) NULL ,
       Zip VARCHAR(200)  NULL,
       Website VARCHAR(200) NULL ,
       Facebook VARCHAR(200) NULL ,
       Twitter VARCHAR(200) NULL ,
       Youtube VARCHAR(200) NULL ,
       OtherMedia VARCHAR(200) NULL  
       );
       ''')
    cursor.execute("SELECT to_regclass('public.Markets_info');")
    result = cursor.fetchone()
    if result is not None:
       print("Таблица Markets_info создана успешно ")
    else:
       print("Ошибка создания таблицы Markets_info ")
    conn_SQL.commit()
    try:
      cursor.execute('''
       ALTER TABLE Farm_markets
       ADD CONSTRAINT fk_FMID_id
       FOREIGN KEY (FMID)
       REFERENCES Markets_info(FMID);
       ''')
      print("Внешние связи построны")
    except:
      print("Ошибка построения внешних связей")
    conn_SQL.commit()
    conn_SQL.close()

#функция перевода информации из CSV-файла в SQL
def opn_CSV(dbname_name,user_name,password_name,host_name):
  print("Запуск CSV модуля")
  conn_SQL =  psycopg2.connect(dbname=dbname_name, user=user_name, password=password_name, host=host_name, port="5432")
  cursor = conn_SQL.cursor()
  file_name=input('введите адрес файа csv: ')
  csv_ferm=open(file_name, mode='r',encoding='utf-8')
  read_csv = csv.reader(csv_ferm)
  next(read_csv)
  if read_csv is not None:
    print("Файл открыт")
# Чтение строк
    for row_csv in read_csv:
      cursor.execute("INSERT INTO Markets_info (FMID,MarketName,Street,City,Country,State_US,Zip,Website,Facebook,Twitter,Youtube,OtherMedia) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row_csv[0],row_csv[1],row_csv[7],row_csv[8],row_csv[9],row_csv[10],row_csv[11],row_csv[2],row_csv[3],row_csv[4],row_csv[5],row_csv[6]))
      cursor.execute("INSERT INTO Farm_markets (FMID,Season1Date,Season1Time,Season2Date,Season2Time,Season3Date,Season3Time,Season4Date,Season4Time,x,y,Location,Credit,WIC,WICcash,SFMNP,SNAP,Organic,Bakedgoods,Cheese,Crafts,Flowers,Eggs,Seafood,Herbs,Vegetables,Honey,Jams,Maple,Meat,Nursery,Nuts,Plants,Poultry,Prepared,Soap,Trees,Wine,Coffee,Beans,Fruits,Grains,Juices,Mushrooms,PetFood,Tofu,WildHarvested,Update_Time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (row_csv[0],row_csv[12],row_csv[13],row_csv[14],row_csv[15],row_csv[16],row_csv[17],row_csv[18],row_csv[19],row_csv[20],row_csv[21],row_csv[22],row_csv[23],row_csv[24],row_csv[25],row_csv[26],row_csv[27],row_csv[28],row_csv[29],row_csv[30],row_csv[31],row_csv[32],row_csv[33],row_csv[34],row_csv[35],row_csv[36],row_csv[37],row_csv[38],row_csv[39],row_csv[40],row_csv[41],row_csv[42],row_csv[43],row_csv[44],row_csv[45],row_csv[46],row_csv[47],row_csv[48],row_csv[49],row_csv[50],row_csv[51],row_csv[52],row_csv[53],row_csv[54],row_csv[55],row_csv[56],row_csv[57],row_csv[58]))
# Сохранение изменений и закрытие соединения
    conn_SQL.commit()
    conn_SQL.close()
    print("Данные сохранены")



# Начало программы
User_name=input('Введите имя пользователя ')
Password_name=input('Введите пароль ')
dbname_name=input('Введите название базы данных ')
Host_name=input('Введите название хоста ')

try:
  crt_SQL(dbname_name,User_name,Password_name,Host_name)
except:
  print("Ошибка создания SQL-файла ")
else:
  try:
    opn_CSV(dbname_name,User_name,Password_name,Host_name)
  except:
    print("Ошибка анализа CSV-файла ")
  else:
    print("Перевод данных из CSV-файла в SQL прошел успешно ")
