import pymysql
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime

now = datetime.now().date()
cities = ['perm', 'spb', 'ekb', 'krasnodar', 'voronezh', 'rnd', 'nn', 'kazan', 'saratov', 'yaroslavl',
          'vladimir', 'samara', 'novosibirsk', 'ryazan', 'volgograd', 'chelyabinsk', 'tyumen', 'ufa']
urls_list = []
name_list = []
price = []
price_opt = []
img_list = []
id_list = []
count_moscow = []
all_count = []
end = []

url = 'https://spb.partsdirect.ru/thermal_equipment/thermal_compound?q=%D1%82%D0%B5%D1%80%D0%BC%D0%BE%D0%BF%D0%B0%D1%81%D1%82%D0%B0&p=all'
headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
    'accept': '*/*',
    'content-type': 'application/json;charset=UTF-8',
}

req = requests.get(url, headers)

with open('partHTML.html', 'w', encoding='utf-8') as file:
    file.write(req.text)
with open('partHTML.html', encoding='utf-8') as file:
    file_content = file.read()
soup = BeautifulSoup(file_content, 'html.parser')
all_id = soup.find(class_= 'cl').find_all(class_='article')
for i in all_id:
    id_list.append(i.text[7:].strip())

def srcp():
    with open('partHTML.html', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    all_product = soup.find(class_="cl").find_all(class_='title')
    urls = soup.find(class_='cl').find_all('a')
    all_price = soup.find(class_="cl").find_all('tr')
    all_price_opt = soup.find(class_="cl").find_all('tr')
    all_img = soup.find(class_='cl').find_all('img')
    for i in urls:
        if i.get('href')[0:7] == '/goods/':
            if 'www.partsdirect.ru'+i.get('href') not in urls_list:
                urls_list.append('www.partsdirect.ru' + i.get('href'))
    for i in all_product:
        name_list.append(i.text)
    for i in all_price:
        d = i.find('div', class_='prices').find('span').text[:-7].strip().replace(' ', '')
        price.append(d)
    for i in all_price_opt:
        d = i.find('div', class_='prices')
        z = d.find(class_='b').text[0:10].strip()
        price_opt.append(z)
    for i in all_img:
        img_list.append(i.get('src'))
    for i in cities:
        url = f'https://{i}.partsdirect.ru/cart/updateGoodCount'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
            'accept': '*/*',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'authority': f'{i}.partsdirect.ru',
            'x-requested-with': 'XMLHttpRequest',
        }
        city = []
        for z in id_list:
            param = {
                'goodId': f'{z}',
                'count': 2
            }
            req = requests.get(url,data=param, headers=headers)
            with open('partBasket.html', 'w', encoding='utf-8') as file:
                file.write(req.text)
            with open('partBasket.html', encoding='utf-8') as file:
                file_content = file.read()
            soup = BeautifulSoup(file_content, 'html.parser')
            site_json = json.loads(soup.text)
            if site_json:
                for k in site_json['items']:
                    city.append(k['max'])
            else:
                city.append(0)
        city = [abs(x1 - x2) for (x1, x2) in zip(count_moscow, city)]

        keys = ['id','name', 'url', 'price', 'price_opt', 'img', 'count']

        zipped = zip(id_list,name_list, urls_list, price, price_opt, img_list, city)
        dicts = [dict(zip(keys, values)) for values in zipped]
        dicts.append({'sum_count': sum(city)})
        end.append({i:dicts})
        print('Закинул город')
    zipped = zip(id_list,name_list, urls_list, price, price_opt, img_list, count_moscow)
    dicts = [dict(zip(keys, values)) for values in zipped]
    dicts.append({'sum_count': sum(count_moscow)})
    end.append({'moscow':dicts})
    with open('data_PArtsDir.json', 'w', encoding='utf-8') as file:
        json.dump(end, file ,ensure_ascii=4, indent=False)
    return end

def get_moscow():
    url = f'https://www.partsdirect.ru/cart/updateGoodCount'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36',
        'accept': '*/*',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'authority': 'www.partsdirect.ru',
        'x-requested-with': 'XMLHttpRequest',
    }
    for i in id_list:
        param = {
            'goodId': f'{i}',
            'count': 2
        }
        req = requests.get(url,data=param, headers=headers)
        with open('partBasket.html', 'w', encoding='utf-8') as file:
            file.write(req.text)
        with open('partBasket.html', encoding='utf-8') as file:
            file_content = file.read()
        soup = BeautifulSoup(file_content, 'html.parser')
        site_json = json.loads(soup.text)
        if site_json:
            for z in site_json['items']:
                count_moscow.append(z['max'])
        else:
            count_moscow.append(0)
    return count_moscow

def inser_DB(len_id):
    connection = pymysql.connect()
    with open('data_PArtsDir.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    numb = 0
    count_sklad = []
    count_nal = []
    while len_id > numb:
        ccc = 0
        nal = 0
        for i in site_json:
            for z in i.values():
                for l in z[numb:numb + 1]:
                    try:
                        ccc += (l['count'])
                        if l['count'] != 0:
                            nal += 1
                    except:
                        pass
        count_sklad.append(ccc)
        count_nal.append(float(round(nal / 19 * 100, 2)) if nal != 0 else
                         0)
        numb += 1
    count = 0
    with connection.cursor() as cursor:
        cursor.execute('SELECT sku_id,brand FROM main_partsdirect_enemy')
        sku_id = dict(cursor.fetchall())
        for i in site_json[:1]:
            for z in i['perm'][:-1]:
                if str(z['id']) in sku_id:
                    try:
                        cursor.execute(
                            f"UPDATE main_partsdirect_enemy  SET title = '{z['name']}', sku_id = {z['id']}, brand = 0, link ='{z['url']}', price = '{z['price'][:-3]}', old_price = '{z['price_opt'][:-3]}',  sale= 0, rating = 0, feedbacks= 0, oonps = 0, stock= {count_sklad[count]}, nal = {count_nal[count]}, image = '{z['img']}', popul = {count} WHERE {z['id']} = sku_id"
                        )
                        count += 1
                    except Exception as ex:
                        print('Не получилось обновить')
                        print(ex)
                else:
                    try:
                        cursor.execute(
                            f"INSERT INTO main_partsdirect_enemy (title,sku_id,brand,link,price,old_price,sale,rating,feedbacks,oonps,stock,nal,image,popul) VALUES ('{z['name']}', {z['id']}, 0, '{z['url']}', '{z['price'][:-3]}', '{z['price_opt'][:-3]}', 0,0,0,0,{count_sklad[count]}, {count_nal[count]}, '{z['img']}',{count})")
                        count += 1
                    except Exception as ex:
                        print('Не получилось добавить')
                        print(ex)

    connection.commit()
    connection.close()

def inser_DB_enemy(len_id):
    connection = pymysql.connect()
    with open('data_PArtsDir.json', encoding='utf-8') as file:
        file_content = file.read()
    soup = BeautifulSoup(file_content, 'html.parser')
    site_json = json.loads(soup.text)
    numb = 0
    count_sklad = []
    count_nal = []
    while len_id > numb:
        ccc = 0
        nal = 0
        for i in site_json:
            for z in i.values():
                for l in z[numb:numb + 1]:
                    try:
                        ccc += (l['count'])
                        if l['count'] != 0:
                            nal += 1
                    except:
                        pass
        count_sklad.append(ccc)
        count_nal.append(float(round(nal / 19 * 100, 2)) if nal != 0 else
                         0)
        numb += 1
    count = 0
    with connection.cursor() as cursor:
        for i in site_json[:1]:
            for z in i['perm'][:-1]:
                # try:
                #     cursor.execute(
                #     f"CREATE TABLE partsdirect_enemy_{z['id']}(ID int NOT NULL AUTO_INCREMENT, date varchar(25) NOT NULL,  price varchar(25) NOT NULL, old_price varchar(25) NOT NULL, sale varchar(25) NOT NULL, rating varchar(25) NOT NULL, feedbacks varchar(25) NOT NULL, oonps varchar(25) NOT NULL, stock varchar(25) NOT NULL, nal varchar(25) NOT NULL, image varchar(25) NOT NULL,popul varchar(25) NOT NULL,PRIMARY KEY (ID))")
                # except Exception as ex:
                #     print('Не получилось создать')
                #     print(ex)
                try:
                    cursor.execute(f"SELECT date,sale FROM partsdirect_enemy_{z['id']}")
                    date_last = dict(cursor.fetchall())
                    if str(now) not in date_last:
                        cursor.execute(
                            f"INSERT INTO partsdirect_enemy_{z['id']} (date,price,old_price,sale,rating,feedbacks,oonps, stock, nal, image, popul) VALUES ('{now}', '{z['price'][:-3]}', '{z['price_opt'][:-3]}', 0, 0, 0, 0 ,{count_sklad[count]}, {count_nal[count]}, '{z['img']}', {count})")
                    count+=1
                except Exception as ex:
                    print(ex)
                    print('Не добавлись данные в страницу')
            connection.commit()
            connection.close()
            
def main():
    get_moscow()
    srcp()
    inser_DB(len(id_list))
    inser_DB_enemy(len(id_list))

if __name__ == "__main__":
    main()




